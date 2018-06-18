const exec = require('child_process').exec;
const execFile = require('child_process').execFile;
const config = require('../config.json');
const xml2js = require('xml2js');
const columnParser = require('node-column-parser');

var currentStatusObj = {};

// ----------- Utility functions ----------------
function strTableToArray(startPoint, tableString) {
    var significantRows = tableString
        .split("\n")
        .splice(startPoint) // Eliminte header rows
        .filter(function (row) {
            return row.length > 0;
        }).map(function (el) {
            return el.trim().split(/\s+/);
        });
    return significantRows;
}

function countJobs(state, qstatRows) {
    return qstatRows.reduce(function (preVal, currentVal) {
        if (currentVal[4] === state) {
            return preVal + 1;
        } else {
            return preVal;
        }
    }, 0);
}

function processHosts(result) {
    var hostList = [];
    var hosts = result.qhost.host;
    for (var i = 0; i < hosts.length; i++) {
        var h = hosts[i];
        if (isQueueHost(h)) {
            hostList.push(getHostObj(h));
        }
    }
    return hostList;
}

function isQueueHost(h) {
    var matchQueue = false;
    if (h.queue != null) {
        for (var j = 0; j < h.queue.length; j++) {
            var q = h.queue[j];
            if (q.$.name === config.queue) {
                matchQueue = true;
                break;
            }
        }
    }
    return matchQueue;
}

function getHostObj(h) {
    var host = {};
    host.name = h.$.name;
    // console.log(h);
    for (var i = 0; i < h.hostvalue.length; i++) {
        var hv = h.hostvalue[i];
        if (hv.$.name != 'arch_string') {
            var value = hv._;
            var units = value.slice(value.length - 1);
            var mult = 1;
            if (units == 'K') {
                mult = 1024;
            }
            if (units == 'M') {
                mult = 1024 * 1024;
            }
            if (units == 'G') {
                mult = 1024 * 1024 * 1024;
            }
            if (units == 'T') {
                mult = 1024 * 1024 * 1024 * 1024;
            }
            if (value == '-') {
                value = 0;
            }
            host[hv.$.name] = parseFloat(value) * mult;
        }
    }
    return host;
}

// --------------------------------------------

function check() {
    //var qstat = "qstat -q "+config.queue;
    var qstat = "squeue -r";
    //var qhost = "qhost -q -xml";
    var qhost = "sinfo";
    //console.log(qstat);
    //console.log(qhost);
    exec(qstat, function (error, stdout, stderr) {
		console.log(stdout);
        var qstatRows = strTableToArray(1, stdout);
        //console.log(qstatRows)
        //var numberRunningJobs = countJobs("r", qstatRows);
        //var numberWaitingJobs = countJobs("qw", qstatRows);
        var numberRunningJobs = countJobs("R", qstatRows);
        var numberWaitingJobs = countJobs("PD", qstatRows);
        //console.log(numberRunningJobs);
        //console.log(numberWaitingJobs);

        exec(qhost, function (error, stdout, stderr) {
		//console.log(JSON.stringify(stdout).NODES);
		var sinfoHost = strTableToArray(1, stdout);
		//added
		//console.log(stdout('NODES'));
		var resultObj = {};
    resultObj.jobsRunning = numberRunningJobs; // add the number of jobs to the object
    resultObj.jobsWaiting = numberWaitingJobs; // add the number of jobs to the object
    var cpuUsageCMD ="maintenance/cpuUsage.sh";
    exec(cpuUsageCMD, (error, stdout, stderr) => {
      console.log("cpuUsageMem is ");
      var cpuUsage = stdout/100;
      resultObj.cpuUsage  = cpuUsage;
    });
    var memUsage ="maintenance/memUsage.sh";
    exec(memUsage, (error, stdout, stderr) => {
      console.log("cpuUsage is ");
      var memUsage = stdout/100;
      resultObj.memUsage  = memUsage;
    });

  currentStatusObj = resultObj;

//fin added
            xml2js.parseString(stdout, function (err, result) {
                if (result != null) {
                    var hosts = processHosts(result);

                    var sum_mem = 0;
                    var sum_cpu = 0;
                    for (var i = 0; i < hosts.length; i++) {
                        var h = hosts[i];
                        sum_mem += h.mem_used / h.mem_total;
                        sum_cpu += h.load_avg / h.num_proc;
                    }

                    var resultObj = {};
                    resultObj.cpuUsage = sum_cpu / hosts.length;
                    resultObj.memUsage = sum_mem / hosts.length;
                    resultObj.jobsRunning = numberRunningJobs; // add the number of jobs to the object
                    resultObj.jobsWaiting = numberWaitingJobs; // add the number of jobs to the object
                    currentStatusObj = resultObj;
                }
            });
        })
    });
};

check();
setInterval(check, 3000);

module.exports = function (callback) {
    // console.log(currentStatusObj);
    callback(currentStatusObj);
};
