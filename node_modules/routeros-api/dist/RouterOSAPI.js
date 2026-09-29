"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.RouterOSAPI = void 0;
const Connector_1 = require("./connector/Connector");
const Channel_1 = require("./Channel");
const RosException_1 = require("./RosException");
const RStream_1 = require("./RStream");
const crypto = require("crypto");
const debug = require("debug");
const timers_1 = require("timers");
const events_1 = require("events");
const info = debug('routeros-api:api:info');
const error = debug('routeros-api:api:error');
/**
 * Creates a connection object with the credentials provided
 */
class RouterOSAPI extends events_1.EventEmitter {
    /**
     * Constructor, also sets the language of the thrown errors
     *
     * @param {Object} options
     */
    constructor(options) {
        super();
        /**
         * Connected flag
         */
        this.connected = false;
        /**
         * Connecting flag
         */
        this.connecting = false;
        /**
         * Closing flag
         */
        this.closing = false;
        /**
         * Counter for channels open
         */
        this.channelsOpen = 0;
        /**
         * Flag if the connection was held by the keepalive parameter
         * or keepaliveBy function
         */
        this.holdingConnectionWithKeepalive = false;
        this.registeredStreams = [];
        this.setOptions(options);
    }
    /**
     * Set connection options, affects before connecting
     *
     * @param options connection options
     */
    setOptions(options) {
        this.host = options.host;
        this.user = options.user;
        this.password = options.password;
        this.port = options.port || 8728;
        this.timeout = options.timeout || 10;
        this.tls = options.tls;
        this.keepalive = options.keepalive || false;
    }
    /**
     * Tries a connection to the routerboard with the provided credentials
     *
     * @returns {Promise}
     */
    connect() {
        if (this.connecting)
            return Promise.reject('ALRDYCONNECTING');
        if (this.connected)
            return Promise.resolve(this);
        info('Connecting on %s', this.host);
        this.connecting = true;
        this.connected = false;
        this.connector = new Connector_1.Connector({
            host: this.host,
            port: this.port,
            timeout: this.timeout,
            tls: this.tls,
        });
        return new Promise((resolve, reject) => {
            const endListener = (e) => {
                this.stopAllStreams();
                this.connected = false;
                this.connecting = false;
                if (e)
                    reject(e);
            };
            this.connector.once('error', endListener);
            this.connector.once('timeout', endListener);
            this.connector.once('close', () => {
                this.emit('close');
                endListener();
            });
            this.connector.once('connected', () => {
                this.login()
                    .then(() => {
                    this.connecting = false;
                    this.connected = true;
                    this.connector.removeListener('error', endListener);
                    this.connector.removeListener('timeout', endListener);
                    const connectedErrorListener = (e) => {
                        this.connected = false;
                        this.connecting = false;
                        this.emit('error', e);
                    };
                    this.connector.once('error', connectedErrorListener);
                    this.connector.once('timeout', connectedErrorListener);
                    if (this.keepalive)
                        this.keepaliveBy('#');
                    info('Logged in on %s', this.host);
                    resolve(this);
                })
                    .catch((e) => {
                    this.connecting = false;
                    this.connected = false;
                    reject(e);
                });
            });
            this.connector.connect();
        });
    }
    /**
     * Writes a command over the socket to the routerboard
     * on a new channel
     *
     * @param {string|Array} params
     * @param {Array<string|string[]>} moreParams
     * @returns {Promise}
     */
    write(params, ...moreParams) {
        params = this.concatParams(params, moreParams);
        let chann = this.openChannel();
        this.holdConnection();
        chann.once('close', () => {
            chann = null; // putting garbage collector to work :]
            this.decreaseChannelsOpen();
            this.releaseConnectionHold();
        });
        return chann.write(params);
    }
    /**
     * Writes a command over the socket to the routerboard
     * on a new channel and return an event of what happens
     * with the responses. Listen for 'data', 'done', 'trap' and 'close'
     * events.
     *
     * @param {string|Array} params
     * @param {Array<string|string[]>} moreParams
     * @returns {RStream}
     */
    writeStream(params, ...moreParams) {
        params = this.concatParams(params, moreParams);
        const stream = new RStream_1.RStream(this.openChannel(), params);
        stream.on('started', () => {
            this.holdConnection();
        });
        stream.on('stopped', () => {
            this.unregisterStream(stream);
            this.decreaseChannelsOpen();
            this.releaseConnectionHold();
        });
        stream.start();
        this.registerStream(stream);
        return stream;
    }
    /**
     * Returns a stream object for handling continuous data
     * flow.
     *
     * @param {string|Array} params
     * @param {function} callback
     * @returns {RStream}
     */
    stream(params = [], ...moreParams) {
        let callback = moreParams.pop();
        if (typeof callback !== 'function') {
            if (callback)
                moreParams.push(callback);
            callback = null;
        }
        params = this.concatParams(params, moreParams);
        const stream = new RStream_1.RStream(this.openChannel(), params, callback);
        stream.on('started', () => {
            this.holdConnection();
        });
        stream.on('stopped', () => {
            this.unregisterStream(stream);
            this.decreaseChannelsOpen();
            this.releaseConnectionHold();
            stream.removeAllListeners();
        });
        stream.start();
        stream.prepareDebounceEmptyData();
        this.registerStream(stream);
        return stream;
    }
    /**
     * Keep the connection alive by running a set of
     * commands provided instead of the random command
     *
     * @param {string|Array} params
     * @param {function} callback
     */
    keepaliveBy(params = [], ...moreParams) {
        this.holdingConnectionWithKeepalive = true;
        if (this.keptaliveby)
            timers_1.clearTimeout(this.keptaliveby);
        let callback = moreParams.pop();
        if (typeof callback !== 'function') {
            if (callback)
                moreParams.push(callback);
            callback = null;
        }
        params = this.concatParams(params, moreParams);
        const exec = () => {
            if (!this.closing) {
                if (this.keptaliveby)
                    timers_1.clearTimeout(this.keptaliveby);
                this.keptaliveby = setTimeout(() => {
                    this.write(params.slice())
                        .then((data) => {
                        if (typeof callback === 'function')
                            callback(null, data);
                        exec();
                    })
                        .catch((err) => {
                        if (typeof callback === 'function')
                            callback(err, null);
                        exec();
                    });
                }, (this.timeout * 1000) / 2);
            }
        };
        exec();
    }
    /**
     * Closes the connection.
     * It can be openned again without recreating
     * an object from this class.
     *
     * @returns {Promise}
     */
    close() {
        if (this.closing) {
            return Promise.reject(new RosException_1.RosException('ALRDYCLOSNG'));
        }
        if (!this.connected) {
            return Promise.resolve(this);
        }
        if (this.connectionHoldInterval) {
            timers_1.clearTimeout(this.connectionHoldInterval);
        }
        timers_1.clearTimeout(this.keptaliveby);
        this.stopAllStreams();
        return new Promise((resolve) => {
            this.closing = true;
            this.connector.once('close', () => {
                this.connector.destroy();
                this.connector = null;
                this.closing = false;
                this.connected = false;
                resolve(this);
            });
            this.connector.close();
        });
    }
    /**
     * Opens a new channel either for just writing or streaming
     *
     * @returns {Channel}
     */
    openChannel() {
        this.increaseChannelsOpen();
        return new Channel_1.Channel(this.connector);
    }
    increaseChannelsOpen() {
        this.channelsOpen++;
    }
    decreaseChannelsOpen() {
        this.channelsOpen--;
    }
    registerStream(stream) {
        this.registeredStreams.push(stream);
    }
    unregisterStream(stream) {
        this.registeredStreams = this.registeredStreams.filter((registeredStreams) => registeredStreams !== stream);
    }
    stopAllStreams() {
        for (const registeredStream of this.registeredStreams) {
            registeredStream.stop();
        }
    }
    /**
     * Holds the connection if keepalive wasn't set
     * so when a channel opens, ensure that we
     * receive a response before a timeout
     */
    holdConnection() {
        // If it's not the first connection to open
        // don't try to hold it again
        if (this.channelsOpen !== 1)
            return;
        if (this.connected && !this.holdingConnectionWithKeepalive) {
            if (this.connectionHoldInterval)
                timers_1.clearTimeout(this.connectionHoldInterval);
            const holdConnInterval = () => {
                this.connectionHoldInterval = setTimeout(() => {
                    let chann = new Channel_1.Channel(this.connector);
                    chann.on('close', () => {
                        chann = null;
                    });
                    chann
                        .write(['#'])
                        .then(() => {
                        holdConnInterval();
                    })
                        .catch(() => {
                        holdConnInterval();
                    });
                }, (this.timeout * 1000) / 2);
            };
            holdConnInterval();
        }
    }
    /**
     * Release the connection that was held
     * when waiting for responses from channels open
     */
    releaseConnectionHold() {
        // If there are channels still open
        // don't release the hold
        if (this.channelsOpen > 0)
            return;
        if (this.connectionHoldInterval)
            timers_1.clearTimeout(this.connectionHoldInterval);
    }
    /**
     * Login on the routerboard to provide
     * api functionalities, using the credentials
     * provided.
     *
     * @returns {Promise}
     */
    login() {
        this.connecting = true;
        info('Sending 6.43+ login to %s', this.host);
        return this.write('/login', [
            `=name=${this.user}`,
            `=password=${this.password}`,
        ])
            .then((data) => {
            if (data.length === 0) {
                info('6.43+ Credentials accepted on %s, we are connected', this.host);
                return Promise.resolve(this);
            }
            else if (data.length === 1) {
                info('Received challenge on %s, will send credentials. Data: %o', this.host, data);
                const challenge = Buffer.alloc(this.password.length + 17);
                const challengeOffset = this.password.length + 1;
                // Here we have 32 chars with hex encoded 16 bytes of challenge data
                const ret = data[0].ret;
                challenge.write(String.fromCharCode(0) + this.password);
                // To write 32 hec chars to buffer as bytes we need to write 16 bytes
                challenge.write(ret, challengeOffset, ret.length / 2, 'hex');
                const resp = '00' +
                    crypto
                        .createHash('MD5')
                        .update(challenge)
                        .digest('hex');
                return this.write('/login', [
                    '=name=' + this.user,
                    '=response=' + resp,
                ])
                    .then(() => {
                    info('Credentials accepted on %s, we are connected', this.host);
                    return Promise.resolve(this);
                })
                    .catch((err) => {
                    if (err.message === 'cannot log in' ||
                        err.message ===
                            'invalid user name or password (6)') {
                        err = new RosException_1.RosException('CANTLOGIN');
                    }
                    this.connector.destroy();
                    error("Couldn't loggin onto %s, Error: %O", this.host, err);
                    return Promise.reject(err);
                });
            }
            error('Unknown return from /login command on %s, data returned: %O', this.host, data);
            Promise.reject(new RosException_1.RosException('CANTLOGIN'));
        })
            .catch((err) => {
            if (err.message === 'cannot log in' ||
                err.message === 'invalid user name or password (6)') {
                err = new RosException_1.RosException('CANTLOGIN');
            }
            this.connector.destroy();
            error("Couldn't loggin onto %s, Error: %O", this.host, err);
            return Promise.reject(err);
        });
    }
    concatParams(firstParameter, parameters) {
        if (typeof firstParameter === 'string')
            firstParameter = [firstParameter];
        for (let parameter of parameters) {
            if (typeof parameter === 'string')
                parameter = [parameter];
            if (parameter.length > 0)
                firstParameter = firstParameter.concat(parameter);
        }
        return firstParameter;
    }
}
exports.RouterOSAPI = RouterOSAPI;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiUm91dGVyT1NBUEkuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi9zcmMvUm91dGVyT1NBUEkudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7O0FBQ0EscURBQWtEO0FBQ2xELHVDQUFvQztBQUNwQyxpREFBOEM7QUFFOUMsdUNBQW9DO0FBQ3BDLGlDQUFpQztBQUNqQywrQkFBK0I7QUFDL0IsbUNBQXNDO0FBQ3RDLG1DQUFzQztBQUd0QyxNQUFNLElBQUksR0FBRyxLQUFLLENBQUMsdUJBQXVCLENBQUMsQ0FBQztBQUM1QyxNQUFNLEtBQUssR0FBRyxLQUFLLENBQUMsd0JBQXdCLENBQUMsQ0FBQztBQUU5Qzs7R0FFRztBQUNILE1BQWEsV0FBWSxTQUFRLHFCQUFZO0lBZ0Z6Qzs7OztPQUlHO0lBQ0gsWUFBWSxPQUFvQjtRQUM1QixLQUFLLEVBQUUsQ0FBQztRQXZEWjs7V0FFRztRQUNJLGNBQVMsR0FBWSxLQUFLLENBQUM7UUFFbEM7O1dBRUc7UUFDSSxlQUFVLEdBQVksS0FBSyxDQUFDO1FBRW5DOztXQUVHO1FBQ0ksWUFBTyxHQUFZLEtBQUssQ0FBQztRQWlCaEM7O1dBRUc7UUFDSyxpQkFBWSxHQUFXLENBQUMsQ0FBQztRQUVqQzs7O1dBR0c7UUFDSyxtQ0FBOEIsR0FBWSxLQUFLLENBQUM7UUFRaEQsc0JBQWlCLEdBQWMsRUFBRSxDQUFDO1FBU3RDLElBQUksQ0FBQyxVQUFVLENBQUMsT0FBTyxDQUFDLENBQUM7SUFDN0IsQ0FBQztJQUVEOzs7O09BSUc7SUFDSSxVQUFVLENBQUMsT0FBb0I7UUFDbEMsSUFBSSxDQUFDLElBQUksR0FBRyxPQUFPLENBQUMsSUFBSSxDQUFDO1FBQ3pCLElBQUksQ0FBQyxJQUFJLEdBQUcsT0FBTyxDQUFDLElBQUksQ0FBQztRQUN6QixJQUFJLENBQUMsUUFBUSxHQUFHLE9BQU8sQ0FBQyxRQUFRLENBQUM7UUFDakMsSUFBSSxDQUFDLElBQUksR0FBRyxPQUFPLENBQUMsSUFBSSxJQUFJLElBQUksQ0FB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