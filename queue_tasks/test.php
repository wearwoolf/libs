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
$taskStack = CronQueue::getInstance($dataConnection);

//create list of items
for($i = 0; $i<2; $i++){
    $taskStack->add( (time()+60+$i), 'in_minute', 'file_put',array('index' => $i));
}
