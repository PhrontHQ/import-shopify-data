var Montage = require('montage/montage');
var PATH = require("path");
global.XMLHttpRequest = require('xhr2');

var exitCode = 0;

// //From Montage
// Load package
Montage.loadPackage(PATH.join(__dirname, "."), {
    mainPackageLocation: PATH.join(__filename, ".")
})
// Preload montage to avoid montage-testing/montage to be loaded
.then(function (mr) {
    // // Execute
    return mr.async('import-shopify-data');
})

/*
.then(function () {
    console.log('Done');
}, function (err) {
    console.error('Fail', err, err.stack);
    exitCode = 1;
}).then(function () {
    process.exit(exitCode);
})
*/