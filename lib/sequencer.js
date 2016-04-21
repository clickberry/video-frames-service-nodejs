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
 * @param      {string}    file_path  Local file path to save.
 * @param      {Function}  fn         Callback.
 */
function downloadFile(uri, file_path, fn) {
  debug('Downloading ' + uri + ' to ' + file_path);

  var error;
  var file = fs.createWriteStream(file_path)
    .on('finish', function() {
      this.close(function (err) {
        if (err) return fn(err);
        debug('Downloading to file ' + file_path + ' completed.');
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
 * @param      {string}    video_path  Video file patch.
 * @param      {string}    to_dir      Target directory path.
 * @param      {Function}  fn          Callback.
 */
function extractFrames(video_path, to_dir, fn) {
  debug('Extracting frames from video ' + video_path + ' to ' + to_dir);

  try {
    var proc = new ffmpeg(video_path);
    proc.then(function (video) {
      video.fnExtractFrames(to_dir, {format: 'png'}, function (err, files) {
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
 * @param      {<type>}    s3_bucket    S3 bucket name.
 * @param      {string}    s3_dir       S3 directory name (sub-folder).
 * @param      {<type>}    frame_path   Frame local path.
 * @param      {Function}  fn           Callback.
 */
function uploadFrameToS3(s3_bucket, s3_dir, frame_path, fn) {
  var fileStream = fs.createReadStream(frame_path);
  var fileName = path.basename(frame_path);
  var key = s3_dir + '/' + fileName;
  debug('Uploading video frame to the bucket: ' + key);

  var params = {
    Bucket: s3_bucket,
    Key: key,
    ACL: 'public-read',
    Body: fileStream,
    ContentType: 'image/jpeg'
  };

  s3.upload(params, function (err) {
    if (err) return fn(err);

    var uri = getObjectUri(s3_bucket, key);
    debug('Video frame uploaded: ' + uri);
    fn(null, uri);
  });
}

/**
 * Builds full object URI.
 *
 * @method     getObjectUri
 * @param      {string}  s3_bucket    S3 bucket name.
 * @param      {string}  key_name     Object key name.
 * @return     {string}  Full object URI.
 */
function getObjectUri(s3_bucket, key_name) {
  return 'https://' + s3_bucket + '.s3.amazonaws.com/' + key_name;
}

/**
 * Wraps upload task for later call.
 *
 * @method     uploadTask
 * @param      {string}     s3_bucket  S3 bucket name.
 * @param      {string}     s3_dir     S3 subdirectory name.
 * @param      {string}     file       Image file to upload.
 * @param      {Sequencer}  sequencer  Sequencer object to emit events.
 * @return     {Function}   function (callback) {}
 */
function uploadTask(s3_bucket, s3_dir, file, sequencer) {
  return function (fn) {
    uploadFrameToS3(s3_bucket, s3_dir, file, function (err, uri) {
      if (err) return fn(err);

      // parsing frame idx
      var fileName = path.basename(file);
      var pattern = /_(\d+)/;
      var match = pattern.exec(fileName);
      var idx = parseInt(match[1]);

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
 * @method     uploadBatch
 * @param      {Array}      tasks   Array of tasks: function (callback) {}
 * @return     {Funcvtion}  function (callback) {}
 */
function uploadBatch(tasks) {
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
 * Downloads video and uploads frames to the S3 bucket. 
 * Generates 'frame' event for each uploaded frame.
 *
 * @method     downloadAndExtractToS3
 * @param      {string}    video_uri  Video URI to download and extract frames from.
 * @param      {string}    s3_bucket  S3 bucket name to upload frames to.
 * @param      {Function}  fn         Callback function.
 */
Sequencer.prototype.downloadAndExtractToS3 = function (video_uri, s3_bucket, fn) {
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
  tmp.file(function (err, video_path, fd, cleanup_file) {
    if (err) return handleError(err);

    // download remote file
    downloadFile(video_uri, video_path, function (err) {
      if (err) return handleError(err);

      // create temp dir for frames
      tmp.dir({unsafeCleanup: true}, function (err, dir_path, cleanup_dir) {
        if (err) return handleError(err);

        // extract frames
        extractFrames(video_path, dir_path, function (err, frames) {
          // delete temp file
          cleanup_file();

          if (err) return handleError(err);

          debug(frames.length + ' frames extracted from video ' + video_uri);

          // upload files to s3 and generate events
          var uploadTasks = [];
          var s3_dir = uuid.v4();
          frames.forEach(function (file) {
            uploadTasks.push(uploadTask(s3_bucket, s3_dir, file, sequencer));
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
            batches.push(uploadBatch(batchOfTasks));
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
              cleanup_dir();

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
