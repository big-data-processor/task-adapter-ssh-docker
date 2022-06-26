const os = require('os')
  , path = require('path')
  , EventEmitter = require("events")
  , sshClient = require('ssh2').Client
  , BdpTaskAdapter = require("@big-data-processor/task-adapter-base")
  , utilities = require("@big-data-processor/utilities")
  , fse = utilities.fse
  , sleep = utilities.sleep
  , memHumanize = utilities.humanizeMemory;



class BdpSshDockerAdapter extends BdpTaskAdapter {
  constructor(opt) {
    opt.adapterName = "SSH to Docker";
    opt.adapterAuthor = "Chi Yang: chiyang1118@gmail.com";
    super(opt);
    this.userInfo = os.userInfo();
    this.detach = this.options.detach && this.options.detach.toString().toLowerCase() === 'true' ? true : false;
    this.options.stdoeMode = this.detach ? "watch" : "pipe";
    this.dockerPath = this.options.dockerPath || "docker";
    this.proxyHostIP = this.options.proxyHostIP || undefined;
    this.currentRunningDockerName = "";
    this.sshConfig = {
      host: this.options.remoteIP,
      port: this.options.remotePort || 22,
      username: this.options.remoteUser,
      privateKey: this.options.remoteKeyFile ? fse.readFileSync(this.options.remoteKeyFile) : undefined,
      identity: undefined,
      password: !this.options.remoteKeyFile && this.options.remotePasswd ? this.options.remotePasswd : undefined
    };
    this.sshConfig.identity = this.sshConfig.privateKey;
    this.sshClient = null;
    /**
     * opt: {
     *  remoteUser: ''
     *  , remotePasswd: '' # If empty or ignored, i will use remoteKeyFile for authentication
     *  , remoteIP: ''
     *  , remotePort: 22
     *  , remoteKeyFile: ''
     *  , detach: false
     *  , remoteMountPoint: '' # used to perform rsync
     *  , uid:''
     *  , gid: ''
     *  , syncBeforeExit: true
     *  , syncBeforeStart: true
     *  , tmpfs: ''
     *  , syncOptions (not yet implemented; To filter files to synchronize between local PC to remote server.)
     * }
     */
    
  }
  async sshConnect() {
    if (!this.sshClient) {
      this.sshClient = new sshClient();
    }
    let isConnected = false, connCounter = 0;;
    while(!isConnected && connCounter < 20) {
      try {
        await new Promise((resolve, reject) => {
          this.sshClient.removeAllListeners('error');
          this.sshClient.removeAllListeners('ready');
          this.sshClient.removeAllListeners('end');
          this.sshClient.on('error', (err) => reject(err)).on('ready', () => resolve()).connect(this.sshConfig);
        });
        isConnected = true;
      } catch(err) {
        isConnected = false;
      }
      connCounter ++;
      if (connCounter >= 20 && !isConnected) { throw "ssh connection failed." }
      await sleep(1000);
    }
    return this.sshClient;
  }
  async sshExec(theCommand) {
    const returnObj = { stdout: '', stderr: '', exitCode: null, signal: null};
    return new Promise((resolve, reject) => {
      this.sshClient.exec(theCommand, (err, stream) => {
        if (err) { return reject(err); }
        stream.on('close', (code, signal) => {
          returnObj.exitCode = code;
          returnObj.signal = signal;
          returnObj.stdout = returnObj.stdout.toString();
          returnObj.stderr = returnObj.stderr.toString();
          resolve(returnObj);
        })
        .on('data', data => returnObj.stdout += data)
        .stderr.on('data', data => returnObj.stderr += data);
      });
    });
  }
  async sshExecOnce(theCommand) {
    await this.sshConnect();
    const returnObj = await this.sshExec(theCommand);
    this.sshClient.end();
    return returnObj;
  }
  async assignPid(jobId) {
    let counter = 0;
    while (counter < 1000) {
      try {
        const execResult = await this.sshExecOnce(`${this.dockerPath} inspect ${jobId} --format={{.State.Pid}}`);
        return execResult.stdout.replace(/\n/g, '');
      } catch(e) {
        await sleep(300);
      }
      counter ++;
    }
    return jobId;
  }
  async beforeStart() {
    await this.sshConnect();
    console.log("ssh connected");
  }
  
  async stopAllJobs() {
    await this.sshConnect();
    for (const jobId of this.runningJobs.keys()) {
      process.stderr.write(`[${new Date().toString()}] Stopping job: ${jobId}` + "\n");
      try {
        await this.sshExec(`${this.dockerPath} stop ${jobId}`);
      } catch (err) {
        console.log(err);
      }
    }
    this.sshClient.end();
  }
  async beforeExit() {
    await this.sshConnect();
    process.stderr.write(`[${new Date().toString()}] Cleaning containers...` + "\n");
    for (const jobId of this.runningJobs.keys()) {
      try {
        await this.sshExec(`${this.dockerPath} rm ${jobId}`);
      } catch (err) {
        console.log(err);
      }
    }
    this.sshClient.end();
  }
  async determineJobProxy(jobObj) {
    if (!jobObj.proxy || !jobObj.proxy.containerPort) { return null; }
    let thePort, requestCounter = 0;
    const jobId = jobObj.jobId;
    process.stdout.write(`[task-adapter-ssh-docker] Get the proxy port from docker ...\n`);
    while (!thePort && requestCounter < 3600 ) {
      await sleep(1000);
      const cmd = `${this.dockerPath} port ${jobId} ${jobObj.proxy.containerPort}`;
      try {
        const dockerPortProc = await this.sshExecOnce(cmd);
        if (dockerPortProc.exitCode === 0) {
          thePort = parseInt(dockerPortProc.stdout.trim().split(':')[1]);
          if (Number.isNaN(thePort)) { thePort = null; }
        }
      } catch(err) {
        console.log(`[cmd ${requestCounter}]: ${cmd}`)
        console.log(err);
      }
      requestCounter ++;
    }
    if (!thePort || !jobObj.proxy.ip) {
      process.stdout.write(`[task-adapter-ssh-docker] Stop the web proxy for this result.\n`);
      return null;
    }
    process.stdout.write(`[task-adapter-ssh-docker] The port is ${thePort}\n`);
    jobObj.proxy = Object.assign({}, jobObj.proxy, {port: thePort})
    return {
      protocol: jobObj.proxy.protocol,
      ip: jobObj.proxy.ip,
      port: thePort,
      pathRewrite: jobObj.proxy.pathRewrite,
      entryPath: jobObj.proxy.entryPath,
      containerPort: jobObj.proxy.containerPort
    };
  }
  async jobOverrides(jobObj) {
    jobObj.cpus = jobObj.option.cpus;
    const {value, unit} = memHumanize(jobObj.option.mem, 0);
    jobObj.mem = String(value) + unit.toLowerCase();
    jobObj.option.volumeMappings = Array.isArray(jobObj.option.volumeMappings) ? jobObj.option.volumeMappings.filter(map => map ? true : false) : [];
    if (!this.argTemplate) {
      this.argTemplate = await fse.readFile(path.join(__dirname, 'arg-recipe.yaml'), "utf8");
    }
    jobObj.option.proxyHostIP = this.proxyHostIP;
    jobObj = await super.parseRecipe(jobObj, this.argTemplate);
    jobObj.command = `${this.dockerPath} ${jobObj.args.join(" ")}`;
    return jobObj;
  }
  async jobDeploy(jobObj) {
    const jobId = jobObj.jobId;
    const runningJob = {
      runningJobId: jobId,
      stdoutStream: null,
      stderrStream: null,
      jobEmitter: new EventEmitter(),
      isRunning: false
    };
    if (this.detach) {
      await this.sshExecOnce(jobObj.command);
    } else {
      const conn = new sshClient();
      conn.on('ready', () => {
        conn.exec(jobObj.command, (err, stream) => {
          if (err) throw err;
          runningJob.isRunning = true;
          stream.on('close', (code, signal) => {
            setTimeout(() => {
              conn.end();
              this.emitJobStatus(jobId, code, signal).catch(console.log);
            });
          });
          runningJob.stdoutStream = stream;
          runningJob.stderrStream = stream.stderr;
        });
      }).connect(this.sshConfig);
    }
    runningJob.runningJobId = await this.assignPid(jobId);
    return runningJob;
  }
  async writeStdout(filePath, content, flag, reportErr) {
    return new Promise((resolve) => {
      try {
        fse.createWriteStream(filePath, {flags: flag}).end(content, resolve);
      } catch(e) {
        console.log(e);
        return reportErr ? reject(e) : resolve();
      }
    });
  }
  async detectJobStatus() {
    if (!this.detach) { return; }
    try {
      await this.sshConnect();
      for (const jobId of this.runningJobs.keys()) {
        const jobObj = this.getJobById(jobId);
        let jobStatus = await this.sshExec(`${this.options.dockerPath} inspect ${jobId} --format={{.State.Status}}`);
        jobStatus = jobStatus.stdout.replace(/\n/g, '');
        let exitCode, theLogResult;
        switch (jobStatus) {
          case 'running':
            const runningJob = this.runningJobs.get(jobId);
            runningJob.isRunning = true;
            const currentDate = (new Date()).toISOString();
            if (jobObj.stdoeTimeStamp === undefined) {
              theLogResult = await this.sshExec(`${this.dockerPath} logs ${jobId} --until ${currentDate}`);
            } else {
              theLogResult = await this.sshExec(`${this.dockerPath} logs ${jobId} --since ${jobObj.stdoeTimeStamp} --until ${currentDate}`);
            }
            try {
              await this.writeStdout(jobObj.stdout, (theLogResult.stdout + theLogResult.stderr), 'a', true);
              jobObj.stdoeTimeStamp = currentDate;
            } catch(e) {
              console.log(e);
            }
            break;
          case 'exited':
            try {
              if (jobObj.stdoeTimeStamp === undefined) {
                theLogResult = await this.sshExec(`${this.dockerPath} logs ${jobId}`);
              } else {
                theLogResult = await this.sshExec(`${this.dockerPath} logs ${jobId} --since ${jobObj.stdoeTimeStamp}`);
              }
              await this.writeStdout(jobObj.stdout, (theLogResult.stdout + theLogResult.stderr), 'a', true);
            } catch(e) {
              console.log(e);
            }
            exitCode = await this.sshExec(`${this.dockerPath} inspect ${jobId} --format={{.State.ExitCode}}`);
            exitCode = Number(exitCode.stdout.replace(/\n/g, ""));
            if (isNaN(exitCode)) { exitCode = 3; }
            await this.emitJobStatus(jobId, exitCode, null);
            break;
          case 'dead':
            try {
              if (jobObj.stdoeTimeStamp === undefined) {
                theLogResult = await this.sshExec(`${this.dockerPath} logs ${jobId}`);
              } else {
                theLogResult = await this.sshExec(`${this.dockerPath} logs ${jobId} --since ${jobObj.stdoeTimeStamp}`);
              }
              await this.writeStdout(jobObj.stdout, (theLogResult.stdout + theLogResult.stderr), 'a', true);
            } catch(e) {
              console.log(e);
            }
            exitCode = await this.sshExec(`${this.dockerPath} inspect ${jobId} --format={{.State.ExitCode}}`);
            exitCode = Number(exitCode.stdout.replace(/\n/g, ""));
            if (isNaN(exitCode)) { exitCode = 3; }
            await this.emitJobStatus(jobId, exitCode, null);
            break;
          default:
            console.log(`Unknown container status: ${jobStatus} (${jobId})`);
        }
      }
      this.sshClient.end();
    }catch(e) {
      console.log(e);
      this.sshClient.end();
    }
  }
}

module.exports = BdpSshDockerAdapter;