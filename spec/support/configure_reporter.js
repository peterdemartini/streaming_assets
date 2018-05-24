'use strict';

const { SpecReporter } = require('jasmine-spec-reporter');

jasmine.getEnv().addReporter(new SpecReporter({
    summary: {
        displayStacktrace: true
    }
}));
