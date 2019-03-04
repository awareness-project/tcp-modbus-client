var Net = require('net');

class TcpModbusClient {

    constructor(options) {
        var context = this;
        context.options = options;
        context.tId = 1;

        if(!options.port)options.port = 502;

        this.requestAttempts = options.requestAttempts?options.requestAttempts:3;
        this.responseTimeout = options.responseTimeout?options.responseTimeout:1000;
        this.banTimeout = options.banTimeout?options.banTimeout:10000;
        this.maxConcurrentRequests = options.concurrentRequests?options.concurrentRequests:20;
        this.maxConcurrentRequestsPerSlave = options.concurrentRequestsPerSlave?options.concurrentRequestsPerSlave:2;

        this.concurrentRequests = 0;
        this.slaves = {};

        this.sentRequests = {};
        this.queuedRequests = [];

        var connect = () => {
            console.log('TCP Modbus client is connecting to: ' + options.host + ':' + options.port);
            this.socket = Net.createConnection({port: options.port, host: options.host}, () => {
                console.log('TCP Modbus client has connected to: ' + options.host + ':' + options.port);
            });

            this.socket.on('data', (data) => {
                if(data.length < 9) return; //minimal length is 7 bytes header + function code and exception code
                let tId = data.readUInt16BE(0);
                let request = this.sentRequests[tId];
                if(request){
                    delete this.sentRequests[tId];
                    clearTimeout(request.responseTimer);
                    this.concurrentRequests--;
                    this.slaves[request.slave].concurrentRequests--;

                    this.rockQueue();

                    if(data[6]!==request.telegramm[6]){request.callback('Response wrong slave');return;}
                    if((data.length === 9)&&(data[7]===(request.telegramm[7] + 0x80))){// Handle exception response
                        switch(data[8]) {
                            case 10://Gateway Path Unavailable - ban immediately
                                if(!this.slaves[request.slave].ban){//
                                    setTimeout(()=>{
                                        this.slaves[request.slave].ban = false; //unban with no errors allowed 1 error = next ban;
                                    }, this.banTimeout);
                                    this.slaves[request.slave].attemptsLeft = 0;
                                    this.slaves[request.slave].ban = true;
                                }
                                request.callback('Gateway Path Unavailable');
                                break;
                            case 11://Gateway Target Device Failed to Respond - do the same if response has timed out - reissue the request and count error
                                if(this.slaves[request.slave].errorsLeft){
                                    this.slaves[request.slave].errorsLeft--;
                                    this.newRequest(request);
                                } else {
                                    if(!this.slaves[request.slave].ban){
                                        setTimeout(()=>{
                                            this.slaves[request.slave].ban = false; //unban with no errors allowed 1 error = next ban;
                                        }, this.banTimeout);

                                        this.slaves[request.slave].ban = true;
                                    }
                                    request.callback('Slave doesn\'t reply');
                                }
                                break;

                            default:
                                this.slaves[request.slave].attemptsLeft = this.requestAttempts;
                                request.callback('Response exception');
                        }
                        return;
                    }

                    if(data.length!==request.expectedRespLen){request.callback('Response wrong length');return;}

                    this.slaves[request.slave].attemptsLeft = this.requestAttempts;
                    request.callback(false, data);
                };
                //console.log('TCP Resource data:' + JSON.stringify(data));
            }).on('close', () => {
                console.log('TCP Modbus client connection closed: ' + options.host + ':' + options.port );
                setTimeout(connect, 5000);

                for (let tId in this.sentRequests){
                    let transaction = this.sentRequests[tId];
                    clearTimeout(transaction.responseTimer);
                    delete this.sentRequests[tId];
                    transaction.callback('TCP connection lost');
                }

                this.queuedRequests.forEach((request)=>{
                    request.callback('TCP connection lost');
                });

                this.queuedRequests = [];
                this.concurrentRequests = 0;
                Object.keys(this.slaves).forEach((key)=>{this.slaves[key].concurrentRequests = 0});

            }).on('error', (error) => {
                console.log('TCP Modbus client connection error: ' + error);
            });

        }

        connect();
    }

    sendRequest(options) {

        if(this.socket.destroyed || this.socket.connecting){
            options.callback('Socket is\'nt connected' );
        } else {
            this.tId++;
            this.tId &= 0xFFFF;

            options.tId = this.tId;

            options.telegramm.writeUInt16BE(options.tId, 0);
            options.telegramm[6] = options.slave;

            this.concurrentRequests++;
            this.slaves[options.slave].concurrentRequests++;

            this.socket.write(options.telegramm, (err)=>{
                if(err){
                    options.callback('Error writing to socket');

                    this.concurrentRequests--;
                    this.slaves[options.slave].concurrentRequests--;

                    this.rockQueue();

                } else {
                    options.responseTimer = setTimeout(()=>{
                        delete this.sentRequests[options.tId];
                        this.concurrentRequests--;
                        this.slaves[options.slave].concurrentRequests--;

                        if(this.slaves[options.slave].errorsLeft){
                            this.slaves[options.slave].errorsLeft--;
                            this.rockQueue();
                            this.newRequest(options);
                        } else {
                            if(!this.slaves[options.slave].ban){
                                setTimeout(()=>{
                                    this.slaves[options.slave].ban = false; //unban with no errors allowed 1 error = next ban;
                                }, this.banTimeout);

                                this.slaves[options.slave].ban = true;
                            }
                            this.rockQueue();
                            options.callback('Slave doesn\'t reply');
                        }
                    }, this.responseTimeout);
                    this.sentRequests[options.tId] = options;
                }
            });
        }
    };

    newRequest(options){

        if((this.concurrentRequests < this.maxConcurrentRequests)
            && (this.slaves[options.slave].concurrentRequests < this.maxConcurrentRequestsPerSlave)
        ){
            this.sendRequest(options);
        } else {
            this.queuedRequests.push(options);
        }
    }

    rockQueue(){
        if(this.queuedRequests.length && (this.concurrentRequests < this.maxConcurrentRequests)){
            let index = this.queuedRequests.findIndex((request)=>{
                return this.slaves[request.slave].concurrentRequests < this.maxConcurrentRequestsPerSlave;
            });
            if(index > -1){
                let request = this.queuedRequests.splice(index,1)[0];
                this.newRequest(request);
                setTimeout(()=>this.rockQueue(),0);
            }
        }
    }

    checkBan(slave, callback){
        if(!this.slaves[slave])this.slaves[slave] = {errorsLeft:this.requestAttempts, ban:false, concurrentRequests: 0};
        if( this.slaves[slave].ban){
            if(typeof callback === "function") setTimeout(callback, 1000, 'Slave is banned');
            return true;
        }
    }

    readHoldings(slave, register, schema, callback) {
        var context = this;

        if(this.checkBan(slave, callback)) return;


        var buf = new Buffer([0x01,0x02,0x00,0x00,0x00,0x06,0x00,0x03,0x00,0x00,0x00,0x00]);//01,02 - transaction id; 00,06 - following data length

        if(!schema){
            var schema = [{itemType:'W',count:count}];
        }

        var count = 0;
        for(var record of schema){
            switch(record.itemType) {
                case 'E'://Empty
                case 'W'://Word
                case 'I'://Int
                case 'B'://Binary
                    count += record.count; //
                    break;
                case 'F'://Float
                case 'DW'://Double word
                case 'DI'://Double int
                    count += 2 * record.count; //
                    break;
                default:
            }
        }

        buf.writeUInt16BE(register, 8);
        buf.writeUInt16BE(count, 10);

        let request = {
            slave,
            telegramm:buf,
            expectedRespLen: 9 + count * 2,
            schema:schema,
            attemptsLeft:context.requestAttempts
        };

        request.callback = (err, receiveBuffer)=>{
            if(err){
                if(typeof callback === "function") setTimeout(callback, 1000, err);
            } else {
                var data = [];
                var dataCount = request.expectedRespLen;
                var dataPos = 9;
                var shemaPos = 0;
                var shemaItemNum = 0;
                while ((dataPos < dataCount) && (shemaPos < schema.length)) {
                    var schemaItem = schema[shemaPos];
                    switch(schemaItem.itemType) {
                        case 'E'://Empty
                            dataPos += 2 * schemaItem.count; //
                            shemaItemNum = schemaItem.count; //skip loose loops
                            break;
                        case 'W'://Word
                            data.push(receiveBuffer.readUInt16BE(dataPos));
                            dataPos += 2;
                            break;
                        case 'I'://Int
                            data.push(receiveBuffer.readInt16BE(dataPos));
                            dataPos += 2;
                            break;
                        case 'B'://Binary
                            var tmp = receiveBuffer.readInt16BE(dataPos);
                            var binArr = [];
                            for(var i = 0; i < 16; i++){
                                binArr.push(tmp & 1 === 1);
                                tmp>>>=1;
                            }
                            data.push(binArr);
                            dataPos += 2;
                            break;
                        case 'F'://Float
                            if(schemaItem.swapWords) swapWords(context.receiveBuffer, dataPos);
                            data.push(receiveBuffer.readFloatBE(dataPos));
                            dataPos += 4;
                            break;
                        case 'DW'://Double Word
                            if(schemaItem.swapWords) swapWords(context.receiveBuffer, dataPos);
                            data.push(context.receiveBuffer.readUInt32BE(dataPos));
                            dataPos += 4;
                            break;
                        case 'DI'://Double Int
                            if(schemaItem.swapWords) swapWords(context.receiveBuffer, dataPos);
                            data.push(context.receiveBuffer.readInt32BE(dataPos));
                            dataPos += 4;
                            break;
                        default:
                            data.push(null);
                            dataPos += 2;
                    }
                    shemaItemNum++;
                    if(shemaItemNum >= schemaItem.count){
                        shemaItemNum = 0;
                        shemaPos++;
                    }
                }
                if(typeof callback === "function") setTimeout(callback, 0, false, data);
            }
        }

        context.newRequest(request);
    };

    writeHoldings(slave, register, schema, data, callback) {
        var context = this;

        if(this.checkBan(slave, callback)) return;

        var context = this;

        if(!schema){
            var schema = [{itemType:'W',count:count}];
        }

        var count = 0;
        for(var record of schema){
            switch(record.itemType) {
                case 'E'://Empty
                case 'W'://Word
                case 'I'://Int
                case 'B'://Binary
                    count += record.count; //
                    break;
                case 'F'://Float
                case 'DW'://Double word
                case 'DI'://Double int
                    count += 2 * record.count; //
                    break;
                default:
            }
        }

        var buf = new Buffer(count * 2 + 13);

        buf.writeUInt16BE(count * 2 + 7, 4);
        buf[6] = slave;
        buf[7] = 16; // write multiple holdings;
        buf.writeUInt16BE(register, 8);
        buf.writeUInt16BE(count, 10);
        buf[12] = count * 2;

        var dataCount = count * 2 + 13;
        var dataPos = 13;
        var dataNum = 0;
        var shemaPos = 0;
        var shemaItemNum = 0;
        while ((dataPos < dataCount) && (shemaPos < schema.length)) {
            var schemaItem = schema[shemaPos];
            switch(schemaItem.itemType) {
                case 'E'://Empty
                    dataPos += 2 * schemaItem.count; //
                    shemaItemNum = schemaItem.count; //skip loose loops
                    break;
                case 'W'://Word
                    buf.writeUInt16BE(data[dataNum], dataPos);
                    dataPos += 2;
                    dataNum ++;
                    break;
                case 'I'://Int
                    buf.writeInt16BE(data[dataNum], dataPos);
                    dataPos += 2;
                    dataNum ++;
                    break;
                case 'B'://Binary
                    var binArr = data[dataNum];
                    var tmp = 0;
                    for(var i = 15; i > -1; i--){
                        tmp<<=1;
                        tmp += binArr[i]
                    }
                    buf.writeUInt16BE(tmp, dataPos);
                    dataPos += 2;
                    dataNum ++;
                    break;
                case 'F'://Float
                    buf.writeFloatBE(data[dataNum], dataPos);
                    if(schemaItem.swapWords) swapWords(buf, dataPos);
                    dataPos += 4;
                    dataNum ++;
                    break;
                case 'DW'://Double Word
                    buf.writeUInt32BE(data[dataNum], dataPos);
                    if(schemaItem.swapWords) swapWords(buf, dataPos);
                    dataPos += 4;
                    dataNum ++;
                    break;
                case 'DI'://Double Int
                    buf.writeInt32BE(data[dataNum], dataPos);
                    if(schemaItem.swapWords) swapWords(buf, dataPos);
                    dataPos += 4;
                    dataNum ++;
                    break;
                default:
                    dataPos += 2;
                    dataNum ++;
            }
            shemaItemNum++;
            if(shemaItemNum >= schemaItem.count){
                shemaItemNum = 0;
                shemaPos++;
            }
        }

        let request = {
            slave,
            telegramm:buf,
            expectedRespLen: 12,
            schema:null,
            attemptsLeft:context.requestAttempts
        };

        request.callback = (err, receiveBuffer)=>{
            if(err){
                if(typeof callback === "function") setTimeout(callback, 1000, err);
            } else {
                var data = [];
                var data = [receiveBuffer.readUInt16BE(8), receiveBuffer.readUInt16BE(10)]; //1st element = address of the 1st register, 2nd = number of registers written

                if(typeof callback === "function") setTimeout(callback, 0, false, data);
            }
        }

        context.newRequest(request);
    };

}

module.exports = TcpModbusClient;

function swapWords(buf, pos) {
    const i1 = buf[pos];
    const i2 = buf[pos + 1];
    buf[pos] = buf[pos + 2];
    buf[pos + 1] = buf[pos + 3];
    buf[pos + 2] = i1;
    buf[pos + 3] = i2;
}