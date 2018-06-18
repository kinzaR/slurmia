// Internal require

const config = require('./config.json');
const checkconfig = require('./lib/checkconfig.js');
const mail = require('./lib/mail/mail.js');

require('./models/job.js');
require('./models/file.js');
require('./models/user.js');

// Package require
const cluster = require('cluster');
const fs = require('fs');
const exec = require('child_process').exec;
const xml2js = require('xml2js');
const path = require('path');
const shell = require('shelljs');

var mongoose = require("mongoose");
mongoose.Promise = global.Promise;

const Job = mongoose.model('Job');
const User = mongoose.model('User');
const File = mongoose.model('File');

var LOCK = false;

if (cluster.isMaster) {
  var jobWorker;
    var anonymousWorker;

    // Code to run if we're in the master process
    checkconfig(function (err) {
        if (err == null) {

            // Count the machine's CPUs
            // var cpuCount = require('os').cpus().length;
            // var cpuCount = 1;

            // Create a worker for each CPU
            console.log('');
            console.log('Starting daemon worker...');
            console.log('===================');
            // for (var i = 0; i < cpuCount; i += 1) {
            // }

            jobWorker = cluster.fork({
                task: 'job'
            });
            anonymousWorker = cluster.fork({
                task: 'anonymous'
            });

            // Listen for dying workers
            cluster.on('exit', function (worker) {
                // Replace the dead worker, we're not sentimental
                if (worker.id == jobWorker.id) {
                    console.log('Job Worker %d died :(', worker.id);
                    jobWorker = cluster.fork({
                        task: 'job'
                    });
                }
                if (worker.id == anonymousWorker.id) {
                    console.log('Anonymous Worker %d died :(', worker.id);
                    anonymousWorker = cluster.fork({
                        task: 'anonymous'
                    });
                }
            });
        }
    });

} else {
    //  Code to run if we're in a worker process
    /******************************/
    /****** Server instance *******/
    /******************************/

    connect()
        .on('error', console.log)
        .on('disconnected', connect)
        .once('open', listen);

    function listen() {
        console.log('Worker %d running!', cluster.worker.id);

        switch (process.env.task) {
        case "job":
            var interval = setInterval(function () {
                console.log('LOCK is: ' + LOCK);
                if (LOCK == false) {
                    LOCK = true;
                    run(function () {
                        LOCK = false;
                    });
                }
            }, 5000);
            break;

        case "anonymous":
            var interval = setInterval(function () {
                deleteAnonymous();
            }, 1000 * 60 * 60 * 24);
            break;
        default:

        }

    }

    function connect() {
        var options = {
            server: {
                socketOptions: {
                    keepAlive: 120
                }
            },
            config: {
                autoIndex: false
            }
        };
        return mongoose.connect(config.mongodb, options).connection;
    }
}

function run(cb) {
    getDbJobs(function (dbJobs) {
        getSGEQstatJobs(function (qJobs) {
            var ids = Object.keys(dbJobs);
            var c = ids.length;
            if (c > 0) {
                // console.log('---Jobs to check in the Data base---');
                // console.log(ids);
                // console.log('---Jobs to check in the Cluster---');
                // console.log(qJobs);
                // console.log('-------------------');
                for (var qId in dbJobs) {
                  console.log(qId);
                    var dbJob = dbJobs[qId];
                    var qJob = qJobs[qId];
                    console.log(qJob);
                    if (qJob != null) {
                        //The job is on the qstat
                        console.log('dbJob: ' + dbJob);
                        console.log('qJob: ' + qJob);
                        checkSGEQstatJob(dbJob, qJob);
                        c--;
                        if (c == 0) {
                            cb();
                        }
                    } else {
                        //If not in qstat check on qacct
                        checkSGEQacctJob(dbJob, function () {
                            c--;
                            if (c == 0) {
                                cb();
                            }
                        });
                    }
                }
            } else {
                cb();
            }
        });
    });
}

function getDbJobs(cb) {
    //Slurm:PENDING COMPLETED FAILED
    //PENDING, RUNNING, SUSPENDED, COMPLETING, and COMPLETED
    // QUEUED RUNNING DONE ERROR EXEC_ERROR QUEUE_ERROR QUEUE_WAITING_ERROR
    var jobs = {};
    Job.where('status')
        //.in(['QUEUED', 'RUNNING'])
        .in(['QUEUED', 'PENDING', 'RUNNING'])
        .populate('user')
        .populate({
            path: 'folder',
            populate: {
                path: 'files'
            }
        }).exec(function (err, result) {
            for (var i = 0; i < result.length; i++) {
                var job = result[i];
                jobs[job.qId] = job;
            }
            cb(jobs);
        });
}

function getSGEQstatJobs(cb) {
    var jobs = {};
//test Start node-shell-parser
// var shellParser = require('node-shell-parser');
// var child = require('child_process');
//
// var process = child.spawn('sacct -P --format=JobName\,State ');
// var shellOutput = '';
//
// process.stdout.on('data', function (chunk) {
//   shellOutput += chunk;
// });
//
// process.stdout.on('end', function () {
//   console.log("testing the node-shell-parser");
//   console.log(shellParser(shellOutput,separator='|'))
// });
//test end node-shell-parser

//test Start node-column-parser
var columnParser=require("node-column-parser");

var util=require('util');
//console.log("testing the node-column-parser");
    //exec('qstat -xml', function (error, stdout, stderr) {
exec('sacct --format=JobName%100\\,State%100', (error, stdout, stderr) => {
      if (error) {
        console.error(`exec error: ${error}`);
        return;
      }
      var options={};
      var items=columnParser(stdout, options);
      items.splice(0,1);
      // console.log("items.length="+items.length+"\n"+util.inspect(items));
      // //options.headers.splice(0, 1, { header: 'qId', start: 0, end: 2, ltr: true });
      // console.log("HEADERS:\n"+util.inspect(options.headers));
      // //options.headers.splice(0, 1, 'qId');
      // console.log(`stderr: ${stderr}`);
        // if (rows != null) {
        //     var items = [];
        //     var l1 = stdout.toString().split('\n');
        //     var l2 = l1;
        //     console.log('stdout lenght: '+stdout[1]);
        //     console.log('l1 is : '+l1);
        //     console.log('l2 is : '+l2);
        //     console.log(l2.toString());
        //     if (l1 != null) {
        //         items = items.concat(l1);
        //     }
        //     if (l2 != null) {
        //         items = items.concat(l2);
        //     }
        for (var i = 0; i < items.length; i++) {
                        var item = items[i];
                        var jobName = item.JobName;
                        var state = item["State "];
                        jobs[jobName] = {
                            qId: jobName,
                            state: state
                        };
                    }
        // }
        // if (error !== null) {
        //console.log("los jobs del cluster son : ");
        //console.log(jobs);
        if (stderr !== null) {
            var msg = 'exec error: ' + error;
            console.log(msg);
        }
        cb(jobs);
    });
}

function checkSGEQstatJob(dbJob, qJob) {
    console.log("i am checking the state");
    //PENDING, RUNNING, SUSPENDED, COMPLETING, and COMPLETED
    console.log(qJob.state);
    switch (qJob.state) {
      case 'PENDING':
          if (dbJob.status != "RUNNING") {
              dbJob.status = "RUNNING";
              dbJob.save();
              dbJob.user.save();
          }
          break;
      case 'RUNNING':
          if (dbJob.status != "RUNNING") {
              dbJob.status = "RUNNING";
              dbJob.save();
              dbJob.user.save();
          }
          break;
    case 'SUSPENDED':
        if (dbJob.status != "SUSPENDED") {
            dbJob.status = "SUSPENDED";
            dbJob.save();
            dbJob.user.save();
        }
        break;
    case 'COMPLETING':
        if (dbJob.status != "COMPLETING") {
          dbJob.status = "COMPLETING";
          dbJob.save();
          dbJob.user.save();
        }
        break;
    case 'CANCELLED':
      if (dbJob.status != "CANCELLED") {
        dbJob.status = "CANCELLED";
        recordOutputFolder(dbJob.folder, dbJob);
        dbJob.save();
        dbJob.user.save();
        if (dbJob.user.notifications.job == true) {
            notifyUser(dbJob.user.email, dbJob.status, dbJob);
        }
      }
      break;
    case 'FAILED':
            if (dbJob.status != "EXEC_ERROR") {
              dbJob.status = "EXEC_ERROR";
              recordOutputFolder(dbJob.folder, dbJob);
              dbJob.save();
              dbJob.user.save();
              if (dbJob.user.notifications.job == true) {
                  notifyUser(dbJob.user.email, dbJob.status, dbJob);
              }
            }
            break;
    case 'COMPLETED':
        if (dbJob.status != "DONE") {
            //dbJob.status = "DONE";
            //dbJob.save();
            //dbJob.user.save();

            //added from other part just to test
            console.time("time DONE")
            dbJob.status = "DONE";
            recordOutputFolder(dbJob.folder, dbJob);
            dbJob.save();
            dbJob.user.save();
            console.timeEnd("time DONE")
            if (dbJob.user.notifications.job == true) {
                notifyUser(dbJob.user.email, dbJob.status, dbJob);
            }
            //the end of added from other part just to test
        }
        break;
    }
}

function checkSGEQacctJob(dbJob, cb) {
    var qId = dbJob.qId;
    console.log("the qId is : ");
    console.log(qId);
    console.log("--------------command ------------------");
    //exec("qacct -j " + qId, function (error, stdout, stderr) {
    exec("sacct --name " + qId, function (error, stdout, stderr) {
        console.log(stdout);

        if (error == null) {
            var stdoutLines = stdout.split('\n');
            console.log("--------------stdoutLines ------------------");
            console.log(stdoutLines);

            for (var i = 0; i < stdoutLines.length; i++) {
                var line = stdoutLines[i];
                if (line.indexOf('failed') != -1) {
                    var value = line.trim().split('failed')[1].trim();
                    if (value != '0' && dbJob.status != "QUEUE_ERROR") {
                        dbJob.status = "QUEUE_ERROR";
                        recordOutputFolder(dbJob.folder, dbJob);
                        dbJob.save();
                        dbJob.user.save();
                        if (dbJob.user.notifications.job == true) {
                            notifyUser(dbJob.user.email, dbJob.status, dbJob);
                        }
                    }
                } else if (line.indexOf('exit_status') != -1) {
                    var value = line.trim().split('exit_status')[1].trim();
                    if (value != '0' && dbJob.status != "EXEC_ERROR") {
                        dbJob.status = "EXEC_ERROR";
                        recordOutputFolder(dbJob.folder, dbJob);
                        dbJob.save();
                        dbJob.user.save();
                        if (dbJob.user.notifications.job == true) {
                            notifyUser(dbJob.user.email, dbJob.status, dbJob);
                        }
                    } else if (dbJob.status != "DONE") {
                        console.time("time DONE")
                        dbJob.status = "DONE";
                        recordOutputFolder(dbJob.folder, dbJob);
                        dbJob.save();
                        dbJob.user.save();
                        console.timeEnd("time DONE")
                        if (dbJob.user.notifications.job == true) {
                            notifyUser(dbJob.user.email, dbJob.status, dbJob);
                        }
                    }
                }
            }
        } else {
            // var msg = 'exec error: ' + error;
            // console.log(msg);
            // cb(error, dbJob);
        }
        cb();
    });
}

const filesToIgnore = {
    "bower_components": true
}

function recordOutputFolder(folder, dbJob) {
    var folderPath = path.join(config.steviaDir, config.usersPath, folder.path);
    try {
        var folderStats = fs.statSync(folderPath);
        if (filesToIgnore[folder.name] !== true && folderStats.isDirectory()) {
            var filesInFolder = fs.readdirSync(folderPath);
            for (var i = 0; i < filesInFolder.length; i++) {
                var fileName = filesInFolder[i];
                if (filesToIgnore[fileName] !== true) {
                    var filePath = path.join(folderPath, fileName);
                    var fileStats = fs.statSync(filePath);

                    /* Database entry */
                    var type = "FILE";
                    if (fileStats.isDirectory()) {
                        type = "FOLDER";
                    }
                    var file = new File({
                        name: fileName,
                        user: folder.user,
                        parent: folder,
                        type: type,
                        path: path.join(folder.path, fileName)
                    });
                    folder.files.push(file);
                    file.save();

                    /* RECORD elog and olog */
                    if (dbJob != null) {
                        if (file.name == '.out.job') {
                            dbJob.olog = file;
                        }
                    }
                    /* */

                    if (fileStats.isDirectory()) {
                        recordOutputFolder(file);
                    }
                }
            }
            folder.save();

            /* RECORD elog and olog */
            if (dbJob != null) {
                for (var i = 0; i < folder.files.length; i++) {}
                dbJob.save();
            }
            /* */
        }
    } catch (e) {
        console.log('recordOutputFolder: ');
        console.log(e);
    }
}

function notifyUser(email, status, dbJob) {
    mail.send({
        to: email,
        subject: 'Job notification',
        text: 'Your job called ' + dbJob.name + ' has finished with the next status: ' + status + '\n'
    }, function (error, info) {
        if (error) {
            console.log(error);
        } else {
            console.log('Message sent: ' + info.response);
        }
    });
}

function deleteAnonymous() {

    var date = new Date();
    date.setDate(date.getDate() - 1);
    console.log("Deleting until: " + date);

    User.find({
            'email': 'anonymous@anonymous.anonymous',
            'name': {
                $regex: new RegExp('^' + 'anonymous___')
            },
            'createdAt': {
                $lte: date
            }
        },
        function (err, users) {
            console.log("Deleting: " + users.length + " users");
            if (users.length > 0) {
                var ids = [];
                for (var i = 0; i < users.length; i++) {
                    var user = users[i];
                    ids.push(user._id);

                    var realPath = path.join(config.steviaDir, config.usersPath, user.name);
                    console.log(realPath);
                    try {
                        if (shell.test('-e', realPath)) {
                            shell.rm('-rf', realPath);
                        } else {
                            console.log("NO ENTRA");
                        }
                    } catch (e) {
                        console.log(e);
                        console.log("File fsDelete: file not exists on file system")
                    }
                }

                User.where('_id').in(ids).remove().exec(function () {});
                File.where('user').in(ids).remove().exec(function () {});
                Job.where('user').in(ids).remove().exec(function () {});
            }
        }).populate('home');
}
