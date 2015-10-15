<?php
error_reporting(-1);
require dirname(__FILE__).DIRECTORY_SEPARATOR.'CronQueue.php';
require dirname(__FILE__).DIRECTORY_SEPARATOR.'DataQueue.php';

$dataConnection = DataQueue::getInstance(array(
    'host'  => 'mongodb://localhost:27017',
    'db'    => 'cron_queue',
    'settingsQueue'     => 'settings',
    'waitQueue'         => 'wait',
    'executionQueue'    => 'execution',
    'logsQueue'         => 'logs',
));
$taskStack = CronQueue::getInstance($dataConnection,array('executionUrl' => 'http://mysite.local/exec.php'));

//use with cron or run only in first or last 10 seconds of minute
$taskStack->run();