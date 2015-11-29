import gulp from "gulp";
import lambdaDeploy from "lambda-deploy";

gulp.task("deploy", function () {
    return lambdaDeploy();
});
