var debug = require('debug')('clickberry:video-frames:worker');
var Bus = require('./lib/bus');
var bus = new Bus();
var ffmpeg = require('ffmpeg');
var http = require('http');
var fs = require('fs');
var request = require('request');

var tmp = require('tmp');
tmp.setGracefulCleanup(); // cleanup the temporary files even when an uncaught exception occurs

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
          

          
          process.exit();
          // delete frames dir
          // cleanup_dir();
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