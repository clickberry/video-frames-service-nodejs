// env
if (!process.env.NSQLOOKUPD_ADDRESSES) {
  console.log("NSQLOOKUPD_ADDRESSES environment variable required.");
  process.exit(1);
}

var events = require('events');
var util = require('util');
var nsq = require('nsqjs');
var debug = require('debug')('clickberry:video-frames:worker');

function Bus(options) {
  options = options || {};
  options.nsqlookupdAddresses = options.nsqlookupdAddresses || process.env.NSQLOOKUPD_ADDRESSES;

  var bus = this;
  events.EventEmitter.call(this);

  // register readers
  var lookupdHTTPAddresses = options.nsqlookupdAddresses.split(',');
  debug('lookupdHTTPAddresses: ' + JSON.stringify(lookupdHTTPAddresses));

  // video-creates
  var video_creates_reader = new nsq.Reader('video-creates', 'extract-frames', {
    lookupdHTTPAddresses: lookupdHTTPAddresses
  });
  video_creates_reader.connect();
  video_creates_reader.on('message', function (msg) {
    bus.emit('video-created', msg);
  });
}

util.inherits(Bus, events.EventEmitter);

module.exports = Bus;
