// env
if (!process.env.S3_BUCKET) {
  console.log("S3_BUCKET environment variable required.");
  process.exit(1);
}
var bucket = process.env.S3_BUCKET;

var debug = require('debug')('clickberry:video-frames:worker');
var Bus = require('./lib/bus');
var bus = new Bus();
var ffmpeg = require('ffmpeg');
var http = require('http');
var fs = require('fs');
var uuid = require('node-uuid');
var request = require('request');
var path = require('path');

var tmp = require('tmp');
tmp.setGracefulCleanup(); // cleanup the temporary files even when an uncaught exception occurs

var AWS = require('aws-sdk');
var s3 = new AWS.S3();

var file_url = 'https://clickdev.s3.amazonaws.com/7560b0e0-4f50-464b-bf52-809180b68e1a/92893fd2-6a84-490c-ad1e-3d982154e6d6';

function handleError(err) {
  console.error(err);
}

function downloadFile(url, filePath, fn) {
  debug('Downloading ' + url + ' to ' + filePath);

  var file = fs.createWriteStream(filePath)
    .on('finish', function() {
      this.close(fn);
    });

  request
    .get(url)
    .on('error', function(err) {
      fn(err);
    })
  .pipe(file);
}

function extractFrames(filePath, toPath, fn) {
  debug('Extracting frames from video ' + filePath + ' to ' + toPath);

  try {
    var ffmpeg_proc = new ffmpeg(filePath);
    ffmpeg_proc.then(function (video) {
      video.fnExtractFrameToJPG(toPath, function (err, files) {
        if (err) return fn(err);
        fn(null, files);
      });
    }, function (err) {
      fn(err);
    });
  } catch (e) {
    fn(err);
  }
}

function uploadFrameToS3(bucket_name, dir_name, file_path, fn) {
  var fileStream = fs.createReadStream(file_path);
  var fileName = path.basename(file_path);
  var key = dir_name + '/' + fileName;
  debug('Uploading video frame to the bucket: ' + key);

  var params = {
    Bucket: bucket_name,
    Key: key,
    ACL: 'public-read',
    Body: fileStream,
    ContentType: 'image/jpeg'
  };

  s3.upload(params, function (err) {
    if (err) return fn(err);

    var uri = getBlobUrl(bucket_name, key);
    debug('Video frame uploaded: ' + uri);
    fn(null, uri);
  });
}

function publishFrameEvent(video_id, uri, fn) {
  // parsing frame index
  var fileName = path.basename(uri);
  var pattern = /_(\d+)/;
  var match = pattern.exec(fileName);
  var idx = parseInt(match[1]);

  var frame = {
    uri: uri,
    video_id: video_id,
    frame_idx: idx
  };
  bus.publishVideoFrameCreated(frame, function (err) {
    fn(err);
  });
}

function getBlobUrl(bucket_name, key_name) {
  return 'https://' + bucket_name + '.s3.amazonaws.com/' + key_name;
}

(function downloadAndExtractFrames(url) {
  // create temp file
  tmp.file(function (err, filePath, fd, cleanup_file) {
    if (err) return handleError(err);

    // download remote file
    downloadFile(url, filePath, function (err) {
      if (err) return handleError(err);

      // create temp dir for frames
      tmp.dir({unsafeCleanup: true}, function (err, dirPath, cleanup_dir) {
        if (err) return handleError(err);

        // extract frames
        extractFrames(filePath, dirPath, function (err, files) {
          // delete temp file
          cleanup_file();
          if (err) return handleError(err);

          // upload files to s3 and generate events
          var s3Dir = uuid.v4();
          files.forEach(function (file) {
            uploadFrameToS3(bucket, s3Dir, file, function (err, uri) {
              if (err) return handleError(err);
              // generating event to the bus
              publishFrameEvent(0, uri, function (err) {
                if (err) return handleError(err);
                // delete local file
                fs.unlink(file, function (err) {
                  if (err) return handleError(err);
                });
              });
            });
          });
        });
      });
    });
  });
})(file_url);

// bus.on('video-created', function (msg) {
//   var video = JSON.parse(msg.body);
//   debug('Video created: ' + JSON.stringify(video));

//   try {
//     var ffmpeg_proc = new ffmpeg(video.uri);
//     ffmpeg_proc.then(function (video) {
//       video.fnExtractFrameToJPG('/frames', {
//         frame_rate : 1,
//         number : 5
//       }, function (err, files) {
//         if (err) return console.error(err);
//         console.log('Frames: ' + files);
//       });
//     }, function (err) {
//       console.error('Error: ' + err);
//     });
//   } catch (e) {
//     console.error('Error code: ' + e.code + ', message: ' + e.msg);
//   }
// });

//debug('Listening for messages...');