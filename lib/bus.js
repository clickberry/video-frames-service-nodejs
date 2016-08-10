// env
if (!process.env.NSQLOOKUPD_ADDRESSES) {
  console.log("NSQLOOKUPD_ADDRESSES environment variable required.");
  process.exit(1);
}
if (!process.env.NSQD_ADDRESS) {
  console.log("NSQD_ADDRESS environment variable required.");
  process.exit(1);
}

var events = require('events');
var util = require('util');
var nsq = require('nsqjs');
var debug = require('debug')('clickberry:video-frames:bus');

function Bus(options) {
  options = options || {};
  options.nsqlookupdAddresses = options.nsqlookupdAddresses || process.env.NSQLOOKUPD_ADDRESSES;
  options.nsqdAddress = options.nsqdAddress || process.env.NSQD_ADDRESS;
  options.nsqdPort = options.nsqdPort || process.env.NSQD_PORT || '4150';

  var bus = this;
  events.EventEmitter.call(this);

  // register readers
  var lookupdHTTPAddresses = options.nsqlookupdAddresses.split(',');
  debug('lookupdHTTPAddresses: ' + JSON.stringify(lookupdHTTPAddresses));

  var video_creates_reader = new nsq.Reader('video-segments', 'extract-frames', {
    lookupdHTTPAddresses: lookupdHTTPAddresses
  });
  video_creates_reader.connect();
  video_creates_reader.on('message', function (msg) {
    // touch the message until timeout
    function touch() {
      if (!msg.hasResponded) {
        debug('Touch [%s]', msg.id);
        msg.touch();
        // Touch the message again a second before the next timeout. 
        setTimeout(touch, msg.timeUntilTimeout() - 1000);
      }
    }
    setTimeout(touch, msg.timeUntilTimeout() - 1000);

    bus.emit('segment', msg);
  });

  // register writers
  var writer = new nsq.Writer(options.nsqdAddress, parseInt(options.nsqdPort, 10));
  writer.connect();
  writer.on('ready', function () {
    //bus._writer = writer;
  });
  bus._writer = writer;
}

util.inherits(Bus, events.EventEmitter);

Bus.prototype.publishVideoFrameCreated = function (data, fn) {
  this._writer.publish('video-frames', data, fn);
};

module.exports = Bus;
