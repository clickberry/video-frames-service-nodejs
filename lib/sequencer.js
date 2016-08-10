/**
 * @fileOverview Video frames extracting logic.
 */

var debug = require('debug')('clickberry:video-frames:sequencer');
var ffmpeg = require('ffmpeg');
var fs = require('fs');
var uuid = require('node-uuid');
var request = require('request');
var path = require('path');
var events = require('events');
var util = require('util');
var async = require('async');

var tmp = require('tmp');
tmp.setGracefulCleanup(); // cleanup the temporary files even when an uncaught exception occurs

var AWS = require('aws-sdk');
var s3 = new AWS.S3();

/**
 * A class for extracting frames from a vide file.
 *
 * @class
 */
var Sequencer = function (options) {
  options = options || {};

  /**
   * Additional options
   */
  this.options = options;

  /**
   * Number of images to upload at once.
   */
  this.options.batchSize = this.options.batchSize || 100;
};

/**
 * Sequencer inherits EventEmitter's on()/emit() methods.
 */
util.inherits(Sequencer, events.EventEmitter);


/**
 * Helper function for download file from the URI to the local file path.
 *
 * @method     downloadFile
 * @param      {string}    uri        File URI.
 * @param      {string}    filePath  Local file path to save.
 * @param      {Function}  fn         Callback.
 */
function downloadFile(uri, filePath, fn) {
  debug('Downloading ' + uri + ' to ' + filePath);

  var error;
  var file = fs.createWriteStream(filePath)
    .on('finish', function() {
      if (error) {
        return this.close();
      }
      
      this.close(function (err) {
        if (err) return fn(err);
        debug('Downloading to file ' + filePath + ' completed.');
        fn();
      });
    });

  request
    .get(uri)
    .on('response', function(res) {
      if (200 != res.statusCode) {
        error = new Error('Invalid status code: ' + res.statusCode + ' while downloading ' + uri);
        error.fatal = true;
        fn(error);
      } else if (res.headers['content-type'].indexOf('video/') !== 0) {
        error = new Error('Video file expected by URI: ' + uri + ', but content type ' + res.headers['content-type'] + ' received');
        error.fatal = true;
        fn(error);
      }
    })
    .on('error', function(err) {
      debug('Downloading ' + uri + ' error: ' + err);
      fn(err);
    })
  .pipe(file);
}

/**
 * Extracts frames from the local video file to speicifed folder.
 *
 * @method     extractFrames
 * @param      {string}    videoPath  Video file patch.
 * @param      {string}    toDir      Target directory path.
 * @param      {Function}  fn          Callback.
 */
function extractFrames(videoPath, toDir, fn) {
  debug('Extracting frames from video ' + videoPath + ' to ' + toDir);

  try {
    var proc = new ffmpeg(videoPath);
    proc.then(function (video) {
      video.fnExtractFrameToJPG(toDir, {quality: 2}, function (err, files) {
        if (err) return fn(err);
        fn(null, files);
      });
    }, function (err) {
      fn(err);
    });
  } catch (err) {
    fn(err);
  }
}

/**
 * Uploads video frame image to S3 bucket.
 *
 * @method     uploadFrameToS3
 *
 * @param      {string}    s3Bucket   S3 bucket name.
 * @param      {string}    s3Dir      S3 directory name (sub-folder).
 * @param      {string}    framePath  Frame local path.
 * @param      {Function}  fn         Callback.
 */
function uploadFrameToS3(framePath, s3Bucket, s3Dir, s3FileName, fn) {
  var fileStream = fs.createReadStream(framePath);
  var key = s3Dir + '/' + s3FileName;
  debug('Uploading video frame to the bucket: ' + key);

  var params = {
    Bucket: s3Bucket,
    Key: key,
    ACL: 'public-read',
    Body: fileStream,
    ContentType: 'image/jpeg'
  };

  s3.upload(params, function (err) {
    if (err) return fn(err);

    var uri = getObjectUri(s3Bucket, key);
    debug('Video frame uploaded: ' + uri);

    fn(null, uri);
  });
}

/**
 * Builds full object URI.
 *
 * @method     getObjectUri
 *
 * @param      {string}  s3Bucket  S3 bucket name.
 * @param      {string}  key       Object key name.
 * @return     {string}  Full object URI.
 */
function getObjectUri(s3Bucket, key) {
  return 'https://' + s3Bucket + '.s3.amazonaws.com/' + key;
}

/**
 * Wraps frame processing logic for later call.
 *
 * @method     processFrame
 *
 * @param      {string}     s3Bucket   S3 bucket name.
 * @param      {string}     s3Dir      S3 subdirectory name.
 * @param      {string}     file       Image file to upload.
 * @param      {Sequencer}  sequencer  Sequencer object to emit events.
 * @return     {Function}   function (callback) {}
 */
function processFrame(s3Bucket, s3Dir, file, sequencer) {
  return function (fn) {
    // parsing frame idx
    var fileName = path.basename(file);
    var pattern = /_(\d+)/;
    var match = pattern.exec(fileName);
    var idx = parseInt(match[1]);

    var s3FileName = idx + path.extname(file);

    uploadFrameToS3(file, s3Bucket, s3Dir, s3FileName, function (err, uri) {
      if (err) return fn(err);

      // emit frame event
      var frame = {idx: idx, uri: uri};
      sequencer.emit('frame', frame);

      fn(null, frame);
    });
  };  
}

/**
 * Wraps array of tasks to execute them later parallelly.
 *
 * @method     processBatch
 * @param      {Array}      tasks   Array of tasks: function (callback) {}
 * @return     {Funcvtion}  function (callback) {}
 */
function processBatch(tasks) {
  return function (fn) {
    debug('Executing batch...');
    async.parallel(tasks,
      function (err, results) {
        if (err) return fn(err);

        debug('Batch processed successfully.');
        fn(null, results);
      });
  };
}

/**
 * Downloads video and uploads frames to the S3 bucket. Generates 'frame' event
 * for each uploaded frame.
 *
 * @method     downloadAndExtractToS3
 *
 * @param      {string}    videoId      Original video id.
 * @param      {string}    videoUri     Video URI to download and extract frames
 *                                      from.
 * @param      {string}    s3Bucket     S3 bucket name to upload frames to.
 * @param      {number}    videoFps     Video fps.
 * @param      {number}    requiredFps  Required fps.
 * @param      {Function}  fn           Callback function.
 */
Sequencer.prototype.downloadAndExtractToS3 = function (videoId, videoUri, s3Bucket, videoFps, requiredFps, fn) {
  var sequencer = this;
  fn = fn || function (err) {
    if (err) debug(err);
  };

  var handleError = function (err) {
    // emit error event
    sequencer.emit('error', err);

    // call callback with error
    fn(err);
  };

  // create temp file
  tmp.file(function (err, localVideoPath, fd, cleanupLocalVideo) {
    if (err) return handleError(err);

    // download remote file
    downloadFile(videoUri, localVideoPath, function (err) {
      if (err) return handleError(err);

      // create temp dir for frames
      tmp.dir({unsafeCleanup: true}, function (err, framesPath, cleanupLocalFrames) {
        if (err) return handleError(err);

        // extract frames
        extractFrames(localVideoPath, framesPath, function (err, frames) {
          // delete temp file
          cleanupLocalVideo();

          if (err) return handleError(err);

          debug(frames.length + ' frames extracted from the video (FPS ' + videoFps + '): ' + videoUri);

          // filtering frames according to fps
          var idxMultiplier = Math.round(videoFps / requiredFps);
          frames = frames.filter(function (_, i) {
            if (i % idxMultiplier !== 0) return false;
            return true;
          });

          debug(frames.length + ' frames remain for the FPS: ' + requiredFps);

          // upload files to s3 and generate events
          var uploadTasks = [];
          var s3Dir = videoId + '/' + path.basename(videoUri, path.extname(videoUri));
          frames.forEach(function (file) {
            uploadTasks.push(processFrame(s3Bucket, s3Dir, file, sequencer));
          });

          // break tasks into the batches
          var batches = [];
          var batchSize = sequencer.options.batchSize;
          var batchNumber = Math.floor(uploadTasks.length / batchSize) + 1;
          var i;
          for (i = 0; i < batchNumber; i++) {
            var start = i * batchSize;
            var end = Math.min((i + 1) * batchSize, uploadTasks.length);
            var batchOfTasks = uploadTasks.slice(start, end);
            batches.push(processBatch(batchOfTasks));
          }

          // execute batches serially
          debug('Video frames uploading broken into ' + batches.length + ' batches');
          async.series(batches,
            function (err, results) {
              if (err) return handleError(err);

              var res = {};
              results.forEach(function (b) {
                b.forEach(function (f) {
                  res[f.idx] = f.uri;
                });
              });

              debug('All video frames uploaded to S3');

              // remove local files
              cleanupLocalFrames();

              // emit end event
              sequencer.emit('end', res);

              fn(null, res);
            });
        });
      });
    });
  });
};

module.exports = Sequencer;
