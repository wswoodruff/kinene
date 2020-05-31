'use strict';

const { promises: Fs } = require('fs');

const Bounce = require('@hapi/bounce');
const Hoek = require('@hapi/hoek');
const Wreck = require('@hapi/wreck');

const Ahem = require('ahem');
const Schmervice = require('schmervice');

const AwschomePlugin = require('awschome');

const LOGS_SOURCE = 'kinene';

const internals = {};

module.exports = class Kinene extends Schmervice.Service {

    async initialize() {

        const { kinesis, kinesisBackupUrl, testMode } = this.options;

        // Tell Awschome to only instance the kinesis sdk service
        const Awschome = await Ahem.instance(AwschomePlugin, { kinesis });

        const { kinesisService } = Awschome.services();
        this.kinesis = kinesisService;

        if (!kinesisBackupUrl) {
            this.localLog(['warn'], 'No options.kinesisBackupUrl specified');
        }

        // NOTE TODO setup like this, in prod if this backup goes down everything goes down
        // We need to make something more resilient here in case it goes down
        if (kinesisBackupUrl && !testMode) {

            let statusCode;

            try {
                // Ping the /health route
                const rawResult = await Wreck.request('GET', `${kinesisBackupUrl}/health`);
                statusCode = rawResult.statusCode;
                Hoek.assert(statusCode === 200);
            }
            catch (error) {
                this.log(['error', 'kinesis-backup'], {
                    error: `${error}\n\nKinesis backup service failed healthcheck, Status code: ${statusCode}`
                });
            }
        }
    }

    async log(tags, data) {

        this.localLog(tags, data);

        try {
            await this.kinesisLog(tags, data);
        }
        catch (err) {
            this.localLog(['error', 'kinene', 'log'], err.message);
        }
    }

    localLog(tags, data) {

        // 'data' is optional
        this.server.log(tags, data);
    }

    // TODO make a 'kinesisLogBatch' func that calls 'putRecords'
    async kinesisLog(tags, logInfo) {

        this._ensureConfiguration();

        const { kinesisBackupUrl, testMode } = this.options;

        logInfo = {
            tags,
            ...logInfo,
            source: logInfo.source || LOGS_SOURCE,
            isTest: testMode
        };

        try {
            await this.kinesis.putRecord(logInfo);

            if (testMode) {
                this.server.log(['sent-to-kinesis'], logInfo);
            }
        }
        catch (err) {

            if (!kinesisBackupUrl) {
                this.localLog(['error'], {
                    error: `Kinesis failed; no kinesisBackupUrl provided.\n\nError: ${err.message}`
                });

                return;
            }

            this.localLog(['error'], {
                error: `Kinesis failed: ${err.message}`
            });

            try {
                const rawResult = await Wreck.request('POST', `${kinesisBackupUrl}/log`, {
                    payload: {
                        source: LOGS_SOURCE,
                        logInfo
                    }
                });

                Hoek.assert(rawResult.statusCode === 200, 'Comms to kinesis backup failed');
            }
            catch (backupErr) {

                this.localLog(['error', 'kinene', 'putRecords', 'kinesis-backup'], {
                    msg: 'Kinesis backup failed',
                    logInfo,
                    error: backupErr
                });

                throw backupErr;
            }
        }
    }

    putTestRecords(...args) {

        return this.kinesis.putTestRecords(...args);
    }

    // TODO in addition to this local 'append', it's worth sending this data to kinesis with some redacted info or something
    async appendLocal({ id, msg }) {

        const { logRootPath } = this.options;

        if (!logRootPath) {
            return this.log(['error'], { error: 'Tried to log without providing options.logRootPath' });
        }

        const filePath = `${this.options.logRootPath}/${id}.log`;

        try {
            await Fs.access(filePath, Fs.F_OK);
        }
        catch (err) {
            Bounce.ignore(err, { code: 'ENOENT' });
            await Fs.writeFile(filePath, '');
        }

        await Fs.appendFile(filePath, msg);
    }

    formatRecord(record) {

        const { localeStringCountry, localeStringTimezone, localeStringAbbrev } = this.options;
        const { arrayIfy } = internals;
        const now = new Date();

        return {
            ...record,
            tags: record.tags ? arrayIfy(record.tags) : [],
            timestamp: record.timestamp || now.getTime(),
            timestampReadable: record.timestampReadable || `${now.toLocaleString(localeStringCountry || 'en-US', { timeZone: localeStringTimezone || 'America/New_York' })} ${localeStringAbbrev || (localeStringTimezone ? '' : (this.isDstObserved ? 'EDT' : 'EST'))}`
        };
    }

    _ensureConfiguration() {

        Hoek.assert(this.kinesis && this.streamName, 'kinene is improperly configured');
    }
};

internals.arrayIfy = (val) => [].concat(val);

// Grabbed this isDstObserved stuff from https://stackoverflow.com/questions/11887934/how-to-check-if-the-dst-daylight-saving-time-is-in-effect-and-if-it-is-whats#answer-11888430
internals.stdTimezoneOffset = () => {

    const d = new Date();
    const jan = new Date(d.getFullYear(), 0, 1);
    const jul = new Date(d.getFullYear(), 6, 1);
    return Math.max(jan.getTimezoneOffset(), jul.getTimezoneOffset());
};

// Grabbed this isDstObserved stuff from https://stackoverflow.com/questions/11887934/how-to-check-if-the-dst-daylight-saving-time-is-in-effect-and-if-it-is-whats#answer-11888430
internals.isDstObserved = () => {

    const { stdTimezoneOffset } = internals;

    const d = new Date();
    return d.getTimezoneOffset() < stdTimezoneOffset();
};
