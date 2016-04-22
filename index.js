// env
if (!process.env.S3_BUCKET) {
  console.log("S3_BUCKET environment variable required.");
  process.exit(1);
}
var bucket = process.env.S3_BUCKET;

var debug = require('debug')('clickberry:video-frames:worker');
var Bus = require('./lib/bus');
var bus = new Bus();
var Sequencer = require('./lib/sequencer');

function handleError(err) {
  console.error(err);
}

function publishFrameEvent(video_id, frame_idx, frame_uri, fn) {
  var frame = {
    uri: frame_uri,
    video_id: video_id,
    frame_idx: frame_idx
  };
  bus.publishVideoFrameCreated(frame, function (err) {
    fn(err);
  });
}

bus.on('video-created', function (msg) {
  var video = JSON.parse(msg.body);
  debug('Video created: ' + JSON.stringify(video));

  // extracting and uploading frames
  var sequencer = new Sequencer()
    .on('frame', function (frame) {
      // generate frame event
      publishFrameEvent(video.id, frame.idx, frame.uri, function (err) {
        if (err) handleError(err);
      });
    })
    .on('error', function(err) {
      handleError(err);
    });
  sequencer.downloadAndExtractToS3(video.uri, bucket, function (err) {
    if (err && !err.fatal) {
      // re-queue the message again if not fatal
      debug('Video processing failed (' + video.uri +  '), skipping the file: ' + err);
      return;
    }

    debug('Video processing completed successfully: ' + video.uri);
    msg.finish();
  });
});

debug('Listening for messages...');