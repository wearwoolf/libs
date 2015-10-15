<?php
error_reporting(-1);
require dirname(__FILE__).DIRECTORY_SEPARATOR.'CronQueue.php';
require dirname(__FILE__).DIRECTORY_SEPARATOR.'DataQueue.php';

//get vars
$id = @$_GET['id'];
if( empty($id) ){
    exit;
}

$dataConnection = DataQueue::getInstance(array(
    'host'  => 'mongodb://localhost:27017',
    'db'    => 'cron_queue',
    'settingsQueue'     => 'settings',
    'waitQueue'         => 'wait',
    'executionQueue'    => 'execution',
    'logsQueue'         => 'logs',
));
$taskStack = CronQueue::getInstance($dataConnection);

$taskStack->logData('exec',array('id' => $id));

//get data
$item = $dataConnection->getExecutionOne($id);

//do action
if( !empty($item) and !empty($item['action']) ){

    $taskStack->logData('exec',array('item' => $item));

    switch($item['action']){
        case 'makeQueue':
            $taskStack->makeQueue($item['flag'], $item['exec_time']);
            break;
        case 'file_put':
            file_put_contents(
                dirname(__FILE__).DIRECTORY_SEPARATOR.'test_file_put',
                date('Y-m-d H:i:s').' execution_time:'.date('Y-m-d H:i:s', $item['exec_time']).' index:'.$item['data']['index']."\n",
                FILE_APPEND
            );
            break;
        default:
            exit;
    }

    $taskStack->remove($item);
}
