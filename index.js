// env
if (!process.env.S3_BUCKET) {
  console.log("S3_BUCKET environment variable required.");
  process.exit(1);
}
var bucket = process.env.S3_BUCKET;

var fps = parseInt(process.env.FPS, 10) || 2; // 2 frames per second

var debug = require('debug')('clickberry:video-frames:worker');
var Bus = require('./lib/bus');
var bus = new Bus();
var Sequencer = require('./lib/sequencer');

function handleError(err) {
  console.error(err);
}

function publishFrameEvent(videoId, segmentIdx, frameIdx, frameUri, fn) {
  var data = {
    videoId: videoId,
    segmentIdx: segmentIdx,
    frameIdx: frameAbsoluteIdx,
    uri: frameUri
  };

  bus.publishVideoFrameCreated(data, fn);
}

bus.on('segment', function (msg) {
  var segment = JSON.parse(msg.body);
  debug('New segment: ' + JSON.stringify(segment));

  // extracting and uploading frames
  var sequencer = new Sequencer()
    .on('frame', function (frame) {
      // generate frame event
      publishFrameEvent(segment.videoId, segment.segmentIdx, frame.idx, frame.uri, function (err) {
        if (err) handleError(err);
      });
    })
    .on('error', function(err) {
      handleError(err);
    });

  sequencer.downloadAndExtractToS3(segment.videoId, segment.uri, bucket, segment.fps, fps, function (err) {
    if (err && !err.fatal) {
      // re-queue the message again if not fatal
      debug('Video segment processing failed (' + segment.uri +  '), skipping the file: ' + err);
      return;
    }

    debug('Video segment processing completed successfully: ' + segment.uri);
    msg.finish();
  });
});

debug('Listening for messages...');