"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Transmitter = void 0;
const iconv = require("iconv-lite");
const debug = require("debug");
const info = debug('routeros-api:connector:transmitter:info');
const error = debug('routeros-api:connector:transmitter:error');
/**
 * Class responsible for transmitting data over the
 * socket to the routerboard
 */
class Transmitter {
    /**
     * Constructor
     *
     * @param socket
     */
    constructor(socket) {
        /**
         * Pool of data to be sent after the socket connects
         */
        this.pool = [];
        this.socket = socket;
    }
    /**
     * Write data over the socket, if it not writable yet,
     * save over the pool to be ran after
     *
     * @param {string} data
     */
    write(data) {
        const encodedData = this.encodeString(data);
        if (!this.socket.writable || this.pool.length > 0) {
            info('Socket not writable, saving %o in the pool', data);
            this.pool.push(encodedData);
        }
        else {
            info('Writing command %s over the socket', data);
            this.socket.write(encodedData);
        }
    }
    /**
     * Writes all data stored in the pool
     */
    runPool() {
        info('Running stacked command pool');
        let data;
        while (this.pool.length > 0) {
            data = this.pool.shift();
            this.socket.write(data);
        }
    }
    /**
     * Encode the string data that will
     * be sent over to the routerboard.
     *
     * It's encoded in win1252 so any accentuation on foreign languages
     * are displayed correctly when opened with winbox.
     *
     * Credits for George Joseph: https://github.com/gtjoseph
     * and for Brandon Myers: https://github.com/Trakkasure
     *
     * @param {string} str
     */
    encodeString(str) {
        if (str === null)
            return String.fromCharCode(0);
        const encoded = iconv.encode(str, 'win1252');
        let data;
        let len = encoded.length;
        let offset = 0;
        if (len < 0x80) {
            data = Buffer.alloc(len + 1);
            data[offset++] = len;
        }
        else if (len < 0x4000) {
            data = Buffer.alloc(len + 2);
            len |= 0x8000;
            data[offset++] = (len >> 8) & 0xff;
            data[offset++] = len & 0xff;
        }
        else if (len < 0x200000) {
            data = Buffer.alloc(len + 3);
            len |= 0xc00000;
            data[offset++] = (len >> 16) & 0xff;
            data[offset++] = (len >> 8) & 0xff;
            data[offset++] = len & 0xff;
        }
        else if (len < 0x10000000) {
            data = Buffer.alloc(len + 4);
            len |= 0xe0000000;
            data[offset++] = (len >> 24) & 0xff;
            data[offset++] = (len >> 16) & 0xff;
            data[offset++] = (len >> 8) & 0xff;
            data[offset++] = len & 0xff;
        }
        else {
            data = Buffer.alloc(len + 5);
            data[offset++] = 0xf0;
            data[offset++] = (len >> 24) & 0xff;
            data[offset++] = (len >> 16) & 0xff;
            data[offset++] = (len >> 8) & 0xff;
            data[offset++] = len & 0xff;
        }
        data.fill(encoded, offset);
        return data;
    }
}
exports.Transmitter = Transmitter;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiVHJhbnNtaXR0ZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi9zcmMvY29ubmVjdG9yL1RyYW5zbWl0dGVyLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7OztBQUNBLG9DQUFvQztBQUNwQywrQkFBK0I7QUFFL0IsTUFBTSxJQUFJLEdBQUcsS0FBSyxDQUFDLHlDQUF5QyxDQUFDLENBQUM7QUFDOUQsTUFBTSxLQUFLLEdBQUcsS0FBSyxDQUFDLDBDQUEwQyxDQUFDLENBQUM7QUFFaEU7OztHQUdHO0FBQ0gsTUFBYSxXQUFXO0lBV3BCOzs7O09BSUc7SUFDSCxZQUFZLE1BQWM7UUFWMUI7O1dBRUc7UUFDSyxTQUFJLEdBQWEsRUFBRSxDQUFDO1FBUXhCLElBQUksQ0FBQyxNQUFNLEdBQUcsTUFBTSxDQUFDO0lBQ3pCLENBQUM7SUFFRDs7Ozs7T0FLRztJQUNJLEtBQUssQ0FBQyxJQUFZO1FBQ3JCLE1BQU0sV0FBVyxHQUFHLElBQUksQ0FBQyxZQUFZLENBQUMsSUFBSSxDQUFDLENBQUM7UUFDNUMsSUFBSSxDQUFDLElBQUksQ0FBQyxNQUFNLENBQUMsUUFBUSxJQUFJLElBQUksQ0FBQyxJQUFJLENBQUMsTUFBTSxHQUFHLENBQUMsRUFBRTtZQUMvQyxJQUFJLENBQUMsNENBQTRDLEVBQUUsSUFBSSxDQUFDLENBQUM7WUFDekQsSUFBSSxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsV0FBVyxDQUFDLENBQUM7U0FDL0I7YUFBTTtZQUNILElBQUksQ0FBQyxvQ0FBb0MsRUFBRSxJQUFJLENBQUMsQ0FBQztZQUNqRCxJQUFJLENBQUMsTUFBTSxDQUFDLEtBQUssQ0FBQyxXQUFXLENBQUMsQ0FBQztTQUNsQztJQUNMLENBQUM7SUFFRDs7T0FFRztJQUNJLE9BQU87UUFDVixJQUFJLENBQUMsOEJBQThCLENBQUMsQ0FBQztRQUNyQyxJQUFJLElBQUksQ0FBQztRQUNULE9BQU8sSUFBSSxDQUFDLElBQUksQ0FBQyxNQUFNLEdBQUcsQ0FBQyxF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