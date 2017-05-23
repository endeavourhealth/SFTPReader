var api = (function ($) {

    var test = function(callback) {
        $.ajax({
            url: "api/test"
        }).then(function(data) {
            callback(data);
        });
    };

    return {
      test: test
    };

}(jQuery));

$(document).ready(function() {

    api.test(function(data) {
        $('#message').append(data)
    });

});