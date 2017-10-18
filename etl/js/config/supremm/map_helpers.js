module.exports = {
    encode: function (value) {
        return {
            value: value,
            error: 0
        };
    },
    detectNodeType: {
        formula: function (job) {
            if (job.intel_knl) {
                return module.exports.encode('Intel - Knights Landing');
            }
            if (job.intel_snb) {
                return module.exports.encode('Intel - Sandybridge');
            }
            if (job.intel_wsm) {
                return module.exports.encode('Intel - Westmere');
            }
            return module.exports.encode('Unknown');
        }
    },
    jobType: {
        formula: function (job) {
            var numThreads;
            if (job.lariat && job.lariat.numThreads) {
                numThreads = parseInt(job.lariat.numThreads, 10);
                if (numThreads > 1) {
                    return module.exports.encode('message passing + threads');
                }
                return module.exports.encode('message passing');
            }
            return module.exports.encode('unknown');
        }
    },
    totalTasks: {
        formula: function (job) {
            if (job.lariat && job.lariat.numCores) {
                return this.ref(job, 'lariat.numCores');
            }
            return { value: null, error: 2 };
        }
    },
    activeCoresPerNode: {
        formula: function (job) {
            var nrunning = this.ref(job, 'ps.-.nr_running.max');
            if (nrunning.error) {
                return nrunning;
            }
            // tacc_stats itself is running when taking the measurment so subtract 1.
            return module.exports.encode('' + (nrunning.value - 1));
        }
    }
};
