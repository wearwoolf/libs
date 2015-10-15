<?php
set_time_limit(70);

class CronQueue
{
    /**
     * @var CronQueue
     */
    private static $instance;

    /**
     * Url for sending request with id of task
     * for example http://mysite.com/exec.php will be updated to http://mysite.com/exec.php?id=1234
     * @var
     */
    private $executionUrl;

    /**
     * @var DataQueue
     */
    private $dataConnection;

    /**
     * Array of urls for requests
     * @var
     */
    private $processQueue;

    /**
     * false for disabling log
     * @var bool
     */
    private $doLog = true;

    /**
     * @param $dataConnection
     * @param array $configuration
     * @return CronQueue
     */
    public static function getInstance($dataConnection, $configuration = array())
    {
        $signature = @md5(print_r($configuration,1));
        if( empty(self::$instance[$signature]) ) {
            self::$instance[$signature] = new self($dataConnection, $configuration);
        }
        return self::$instance[$signature];
    }

    private function __construct(DataQueue $dataConnection, $configuration = array())
    {
        /** @var  DataQueue */
        $this->dataConnection = $dataConnection;

        if( !empty($configuration) ){
            $this->executionUrl = (!empty($configuration['executionUrl'])   ? $configuration['executionUrl']    : $this->executionUrl);
            $this->doLog        = (!empty($configuration['doLog'])          ? $configuration['doLog']           : $this->doLog);
        }

    }

    private function __clone(){}
    private function __wakeup(){}

    /**
     * Run work with items in queues
     *
     * @param int $current_time
     */
    public function run($current_time = 0)
    {
        //calculate current time
        $current_time   = (empty($current_time) ? time() : $current_time);
        $second         = (int)date('s', $current_time);

        //special case for cron (can be values 50-60 or 0-10)
        if ($second > 50) {
            sleep((60 - $second));
            $second = (int)date('s');
        }
        if ($second < 10) {
            $second = 0;
        }
        $current_time = $this->mkTime(null, null, $second);

        $this->logData('run',array('current_time' => $current_time));

        //check and prepare task for rebuilding periods
        $this->prepare($current_time);

        //work with items in this minute
        for ($i=0; $i<60; $i++) {
            $allItems = $this->getAll($current_time,2); //get all items for this minute

            //work with items
            if( !empty($allItems) ){
                $this->logData('run_items',array('execution_items' => count($allItems)));

                foreach ($allItems as $k_item => $v_item) {
                    //add request url
                    if ($v_item['exec_time'] == $current_time) {
                        $this->addTask($v_item);
                    }
                }
            }

            //run requests
            $this->runTasks();

            $current_time++;
            sleep(1);
        }
    }

    /**
     * Add item in queue
     *
     * @param int $exec_time    - when run task
     * @param string $flag      - name of period in settings
     * @param string $action    - execution action
     * @param array $data       - any additional data of task
     * @param int $type         - 1 - waiting queue, 2 - execution
     */
    public function add($exec_time, $flag = 'default', $action = 'default', $data = array(), $type = 1)
    {
        $dataInsert                 = array();
        $dataInsert['exec_time']    = $exec_time;
        $dataInsert['flag']         = $flag;
        $dataInsert['action']       = $action;
        $dataInsert['data']         = $data;
        $dataInsert['add_time']     = time();

        /*$dataInsert['log_data']     = array(
            'add_date'  => date('Y-m-d H:i:s'),
            'add_type'  => $type,
            'exec_date' => date('Y-m-d H:i:s',$exec_time),
        );*/

        $this->logData('add',array('type' => $type, 'data' => $dataInsert));

        if( $type == 1 ){
            $this->dataConnection->addWaitQueue($dataInsert);
        }
        if( $type == 2 ){
            $this->dataConnection->addExecutionQueue($dataInsert);
        }
    }

    /**
     * Remove item from queue
     *
     * @param array $data
     * @param int $type     1 - wait queue, 2 - execution queue, 0 - both queue
     */
    public function remove($data = array(),$type = 0)
    {
        $this->logData('remove',array('type' => $type, 'data' => $data));

        if( $type == 0 ){
            $this->dataConnection->removeWaitQueue($data);
            $this->dataConnection->removeExecutionQueue($data);
        }
        if( $type == 1 ){
            $this->dataConnection->removeWaitQueue($data);
        }
        if( $type == 2 ){
            $this->dataConnection->removeExecutionQueue($data);
        }
    }

    /**
     * Move waiting items to execution queue
     *
     * @param $period_name
     * @param $recalculation_time
     * @return bool
     */
    public function makeQueue($period_name, $recalculation_time){
        $dayStart   = $this->mkTime(0,0,0,null,null,null,$recalculation_time);
        $dayEnd     = $this->mkTime(23,59,59,null,null,null,$recalculation_time);

        //get all settings for periods
        $settings       = $this->dataConnection->getSettingsQueue($period_name);
        $periodDuration = 60;
        $minPause       = 0;
        if( !empty($settings) ){
            $periodDuration = $settings['period'];
            $minPause       = $settings['min_pause'];
        }

        //try find start and end of period
        $periodStart = $periodEnd = $periodLeft = 0;
        for($i=$dayStart; $i<$dayEnd; $i+=$periodDuration){
            //skip unnecessary periods
            if( $recalculation_time >= ($i + $periodDuration) ){
                continue;
            }

            if( $i <= $recalculation_time and $i < ($i + $periodDuration) ){
                $periodStart    = $i;
                $periodEnd      = $i + $periodDuration;
                $periodLeft     = $periodEnd-$recalculation_time;
                break;
            }
        }

        //find all items from waiting stack for current period
        $waitItems      = $this->getAll(array($periodStart, $periodEnd), 1, $period_name);
        $waitItemsCount = count($waitItems);

        if( !empty($waitItemsCount) ){
            //move all items to exec stack
            foreach($waitItems as $k_item => $v_item){
                $this->dataConnection->addExecutionQueue($v_item);
                $this->dataConnection->removeWaitQueue(array('_id' => $v_item['_id']));
            }
        }

        //get all items from execution queue
        $execItems      = $this->getAll(array($periodStart, $periodEnd), 2, $period_name);
        $execItemsCount = count($execItems);
        if( !empty($execItemsCount) ){
            //calculate min pause between tasks
            $periodLeft -= 5;//should be less on 5 second for additional protection
            $tmpMinPause = $periodLeft / $execItemsCount;
            if( $tmpMinPause > $minPause ){
                $minPause = $tmpMinPause;
            }

            //prepare new exec time for items
            $nexExecTime = ($periodEnd - $periodLeft);
            foreach($execItems as $k_item => $v_item){
                $this->dataConnection->updateExecutionQueue( $v_item, array('exec_time' => floor($nexExecTime)) );
                $nexExecTime += $minPause;
            }
        }

        return true;
    }


    /**
     * Move items from waiting queue to execution queue
     * @param int $current_time
     * @return bool
     */
    protected function prepare($current_time)
    {
        $dayStart   = $this->mkTime(0,0,0,null,null,null,$current_time);
        $dayEnd     = $this->mkTime(23,59,59,null,null,null,$current_time);

        $tasks  = array();
        $queues = $this->dataConnection->getSettingsQueue();

        //work with settings for each queue
        foreach($queues as $k_queue => $v_queue){
            $recalcPause = (empty($v_queue['recalc_period_pause']) ? $v_queue['period'] : $v_queue['recalc_period_pause']);

            $this->logData('prepare',array('queue_settings' => $v_queue));

            //try find times when should be recalculation
            for($i=$dayStart; $i<$dayEnd; $i+=$recalcPause){
                if( $i < $current_time ){
                    continue;
                }

                //we need only recalculation, which necessary in this minute
                if( $i >= $current_time and $i < ($current_time + 60) ){
                    $tasks[] = array(
                        'exec_time' => ($i == $current_time ? ($current_time+2) : $i),
                        'flag'      => $v_queue['name'],
                        'action'    => 'makeQueue',
                        'data'      => array()
                    );
                }

                //skip times for next minute
                if( $i >= ($current_time + 60) ){
                    break;
                }
            }
        }

        //prepare task for rebuilding queue
        if( !empty($tasks) ){
            foreach($tasks as $k_task => $v_task){
                $this->add($v_task['exec_time'], $v_task['flag'], $v_task['action'], $v_task['data'], 2);
            }
        }
        return true;
    }

    /**
     * Get items from queue
     *
     * @param array|int $current_time
     * @param int $type     1 - wait queue, 2 - execution queue
     * @param string $flag
     * @return array
     */
    protected function getAll($current_time, $type = 0, $flag = '')
    {
        $items  = array();
        $from   = 0;
        $to     = 0;
        if( is_array($current_time) ){
            $from   = intval($current_time[0]);
            $to     = intval($current_time[1]);
        }else{
            $from   = intval($current_time);
            $to     = intval($current_time)+1;
        }

        if( $type == 1){
            $items = $this->dataConnection->getWaitQueue($from, $to, $flag);
        }
        if( $type == 2){
            $items = $this->dataConnection->getExecutionQueue($from, $to, $flag);
        }

        return $items;
    }

    /**
     * Prepare time()
     *
     * @param null $hour
     * @param null $minute
     * @param null $second
     * @param null $month
     * @param null $day
     * @param null $year
     * @param null $time
     * @return int|null
     */
    protected function mkTime($hour = null, $minute = null, $second = null, $month = null, $day = null, $year = null, $time = null){
        $time   = (is_null($time)   ? time()            : $time);

        $hour   = (is_null($hour)   ? date('H',$time)   : $hour);
        $minute = (is_null($minute) ? date('i',$time)   : $minute);
        $second = (is_null($second) ? date('s',$time)   : $second);
        $month  = (is_null($month)  ? date('n',$time)   : $month);
        $day    = (is_null($day)    ? date('j',$time)   : $day);
        $year   = (is_null($year)   ? date('Y',$time)   : $year);

        $time = mktime($hour, $minute, $second, $month, $day, $year);

        return $time;
    }

    /**
     * Add task in queue
     * @param $data
     * @return string
     */
    protected function addTask($data)
    {
        $this->logData('addTask',array('id' => $data['_id']));

        return $this->processQueue[] = $this->executionUrl."?id=".$data['_id'];
    }

    /**
     * Run all tasks from process queue
     * Without waiting answer
     * @return bool
     */
    protected function runTasks()
    {
        if( empty($this->processQueue) ){
            return true;
        }

        $this->logData('runTasks',array('count_process' => count($this->processQueue), 'processes' => $this->processQueue));

        $mh = curl_multi_init();

        $ch_list = array();

        $perRequest = 5;
        for($i=0; $i<count($this->processQueue); $i+=$perRequest) {
            $currentNodes = array_slice($this->processQueue, $i, $perRequest);

            foreach ($currentNodes as $url) {
                $ch = curl_init($url);
                $ch_list[] = $ch;

                curl_setopt_array($ch, array(
                    CURLOPT_RETURNTRANSFER  => 1,
                    CURLOPT_HEADER          => 0,
                    CURLOPT_URL             => $url
                ));
                curl_multi_add_handle($mh, $ch);
            }

            do {
                curl_multi_exec($mh, $active);
                curl_multi_select($mh);
            } while ($active);
        }

        foreach ($ch_list as $ch) {
            curl_multi_remove_handle($mh, $ch);
            curl_close($ch);
        }
        curl_multi_close($mh);

        $this->processQueue = array();

        return true;
    }

    public function logData($operation, $data){
        if($this->doLog){
            $this->dataConnection->addLog($operation, $data);
        }
    }
}