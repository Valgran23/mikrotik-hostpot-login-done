"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.RStream = void 0;
const events_1 = require("events");
const Channel_1 = require("./Channel");
const RosException_1 = require("./RosException");
const timers_1 = require("timers");
const utils_1 = require("./utils");
/**
 * Stream class is responsible for handling
 * continuous data from some parts of the
 * routeros, like /ip/address/listen or
 * /tool/torch which keeps sending data endlessly.
 * It is also possible to pause/resume/stop generated
 * streams.
 */
class RStream extends events_1.EventEmitter {
    /**
     * Constructor, it also starts the streaming after construction
     *
     * @param {Channel} channel
     * @param {Array} params
     * @param {function} callback
     */
    constructor(channel, params, callback) {
        super();
        /** Flag for turning on empty data debouncing */
        this.shouldDebounceEmptyData = false;
        /**
         * If is streaming flag
         */
        this.streaming = true;
        /**
         * If is pausing flag
         */
        this.pausing = false;
        /**
         * If is paused flag
         */
        this.paused = false;
        /**
         * If is stopping flag
         */
        this.stopping = false;
        /**
         * If is stopped flag
         */
        this.stopped = false;
        /**
         * If got a trap error
         */
        this.trapped = false;
        /**
         * Save the current section of the packet, if has any
         */
        this.currentSection = null;
        this.forcelyStop = false;
        /**
         * Store the current section in a single
         * array before sending when another section comes
         */
        this.currentSectionPacket = [];
        this.channel = channel;
        this.params = params;
        this.callback = callback;
    }
    /**
     * Function to receive the callback which
     * will receive data, if not provided over the
     * constructor or changed later after the streaming
     * have started.
     *
     * @param {function} callback
     */
    data(callback) {
        this.callback = callback;
    }
    /**
     * Resume the paused stream, using the same channel
     *
     * @returns {Promise}
     */
    resume() {
        if (this.stopped || this.stopping)
            return Promise.reject(new RosException_1.RosException('STREAMCLOSD'));
        if (!this.streaming) {
            this.pausing = false;
            this.start();
            this.streaming = true;
        }
        return Promise.resolve();
    }
    /**
     * Pause the stream, but don't destroy the channel
     *
     * @returns {Promise}
     */
    pause() {
        if (this.stopped || this.stopping)
            return Promise.reject(new RosException_1.RosException('STREAMCLOSD'));
        if (this.pausing || this.paused)
            return Promise.resolve();
        if (this.streaming) {
            this.pausing = true;
            return this.stop(true)
                .then(() => {
                this.pausing = false;
                this.paused = true;
                return Promise.resolve();
            })
                .catch((err) => {
                return Promise.reject(err);
            });
        }
        return Promise.resolve();
    }
    /**
     * Stop the stream entirely, can't re-stream after
     * this if called directly.
     *
     * @returns {Promise}
     */
    stop(pausing = false) {
        if (this.stopped || this.stopping)
            return Promise.resolve();
        if (!pausing)
            this.forcelyStop = true;
        if (this.paused) {
            this.streaming = false;
            this.stopping = false;
            this.stopped = true;
            if (this.channel)
                this.channel.close(true);
            return Promise.resolve();
        }
        if (!this.pausing)
            this.stopping = true;
        let chann = new Channel_1.Channel(this.channel.Connector);
        chann.on('close', () => {
            chann = null;
        });
        if (this.debounceSendingEmptyData)
            this.debounceSendingEmptyData.cancel();
        return chann
            .write(['/cancel', '=tag=' + this.channel.Id])
            .then(() => {
            this.streaming = false;
            if (!this.pausing) {
                this.stopping = false;
                this.stopped = true;
            }
            this.emit('stopped');
            return Promise.resolve();
        })
            .catch((err) => {
            return Promise.reject(err);
        });
    }
    /**
     * Alias for stop()
     */
    close() {
        return this.stop();
    }
    /**
     * Write over the connection and start the stream
     */
    start() {
        if (!this.stopped && !this.stopping) {
            this.channel.on('close', () => {
                if (this.forcelyStop || (!this.pausing && !this.paused)) {
                    if (!this.trapped)
                        this.emit('done');
                    this.emit('close');
                }
                this.stopped = false;
            });
            this.channel.on('stream', (packet) => {
                if (this.debounceSendingEmptyData)
                    this.debounceSendingEmptyData.run();
                this.onStream(packet);
            });
            this.channel.once('trap', this.onTrap.bind(this));
            this.channel.once('done', this.onDone.bind(this));
            this.channel.write(this.params.slice(), true, false);
            this.emit('started');
            if (this.shouldDebounceEmptyData)
                this.prepareDebounceEmptyData();
        }
    }
    prepareDebounceEmptyData() {
        this.shouldDebounceEmptyData = true;
        const intervalParam = this.params.find((param) => {
            return /=interval=/.test(param);
        });
        let interval = 2000;
        if (intervalParam) {
            const val = intervalParam.split('=')[2];
            interval = parseInt(val, null) * 1000;
        }
        this.debounceSendingEmptyData = utils_1.debounce(() => {
            if (!this.stopped ||
                !this.stopping ||
                !this.paused ||
                !this.pausing) {
                this.onStream([]);
                this.debounceSendingEmptyData.run();
            }
        }, interval + 300);
    }
    /**
     * When receiving the stream packet, give it to
     * the callback
     *
     * @returns {function}
     */
    onStream(packet) {
        this.emit('data', packet);
        if (this.callback) {
            if (packet['.section']) {
                timers_1.clearTimeout(this.sectionPacketSendingTimeout);
                const sendData = () => {
                    this.callback(null, this.currentSectionPacket.slice(), this);
                    this.currentSectionPacket = [];
                };
                this.sectionPacketSendingTimeout = timers_1.setTimeout(sendData.bind(this), 300);
                if (this.currentSectionPacket.length > 0 &&
                    packet['.section'] !== this.currentSection) {
                    timers_1.clearTimeout(this.sectionPacketSendingTimeout);
                    sendData();
                }
                this.currentSection = packet['.section'];
                this.currentSectionPacket.push(packet);
            }
            else {
                this.callback(null, packet, this);
            }
        }
    }
    /**
     * When receiving a trap over the connection,
     * when pausing, will receive a 'interrupted' message,
     * this will not be considered as an error but a flag
     * for the pause and resume function
     *
     * @returns {function}
     */
    onTrap(data) {
        if (data.message === 'interrupted') {
            this.streaming = false;
        }
        else {
            this.stopped = true;
            this.trapped = true;
            if (this.callback) {
                this.callback(new Error(data.message), null, this);
            }
            else {
                this.emit('error', data);
            }
            this.emit('trap', data);
        }
    }
    /**
     * When the channel stops sending data.
     * It will close the channel if the
     * intention was stopping it.
     *
     * @returns {function}
     */
    onDone() {
        if (this.stopped && this.channel) {
            this.channel.close(true);
        }
    }
}
exports.RStream = RStream;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiUlN0cmVhbS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uL3NyYy9SU3RyZWFtLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7OztBQUFBLG1DQUFzQztBQUN0Qyx1Q0FBb0M7QUFDcEMsaURBQThDO0FBQzlDLG1DQUFrRDtBQUNsRCxtQ0FBbUM7QUFFbkM7Ozs7Ozs7R0FPRztBQUNILE1BQWEsT0FBUSxTQUFRLHFCQUFZO0lBNkVyQzs7Ozs7O09BTUc7SUFDSCxZQUNJLE9BQWdCLEVBQ2hCLE1BQWdCLEVBQ2hCLFFBQStEO1FBRS9ELEtBQUssRUFBRSxDQUFDO1FBL0RaLGdEQUFnRDtRQUN4Qyw0QkFBdUIsR0FBWSxLQUFLLENBQUM7UUFFakQ7O1dBRUc7UUFDSyxjQUFTLEdBQVksSUFBSSxDQUFDO1FBRWxDOztXQUVHO1FBQ0ssWUFBTyxHQUFZLEtBQUssQ0FBQztRQUVqQzs7V0FFRztRQUNLLFdBQU0sR0FBWSxLQUFLLENBQUM7UUFFaEM7O1dBRUc7UUFDSyxhQUFRLEdBQVksS0FBSyxDQUFDO1FBRWxDOztXQUVHO1FBQ0ssWUFBTyxHQUFZLEtBQUssQ0FBQztRQUVqQzs7V0FFRztRQUNLLFlBQU8sR0FBWSxLQUFLLENBQUM7UUFFakM7O1dBRUc7UUFDSyxtQkFBYyxHQUFXLElBQUksQ0FBQztRQUU5QixnQkFBVyxHQUFZLEtBQUssQ0FBQztRQUVyQzs7O1dBR0c7UUFDSyx5QkFBb0IsR0FBVSxFQUFFLENBQUM7UUFvQnJDLElBQUksQ0FBQyxPQUFPLEdBQUcsT0FBTyxDQUFDO1FBQ3ZCLElBQUksQ0FBQyxNQUFNLEdBQUcsTUFBTSxDQUFDO1FBQ3JCLElBQUksQ0FBQyxRQUFRLEdBQUcsUUFBUSxDQUFDO0lBQzdCLENBQUM7SUFFRDs7Ozs7OztPQU9HO0lBQ0ksSUFBSSxDQUNQLFFBQThEO1FBRTlELElBQUksQ0FBQyxRQUFRLEdBQUcsUUFBUSxDQUFDO0lBQzdCLENBQUM7SUFFRDs7OztPQUlHO0lBQ0ksTUFBTTtRQUNULElBQUksSUFB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