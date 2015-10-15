<?php

class DataQueue
{
    private static $instance;

    private $connection;
    private $db;
    private $settingsQueue;
    private $waitQueue;
    private $executionQueue;
    private $logsQueue;

    /**
     * @param array $configuration
     * @return DataQueue
     */
    public static function getInstance($configuration = array())
    {
        $signature = @md5(print_r($configuration,1));
        if( empty(self::$instance[$signature]) ) {
            self::$instance[$signature] = new self($configuration);
        }
        return self::$instance[$signature];
    }

    private function __construct($configuration){
        if( !empty($configuration) ){
            $this->db               = (!empty($configuration['db'])  ? $configuration['db']   : $this->db);
            $this->settingsQueue    = (!empty($configuration['settingsQueue'])  ? $configuration['settingsQueue']   : $this->settingsQueue);
            $this->waitQueue        = (!empty($configuration['waitQueue'])      ? $configuration['waitQueue']       : $this->waitQueue);
            $this->executionQueue   = (!empty($configuration['executionQueue']) ? $configuration['executionQueue']  : $this->executionQueue);
            $this->logsQueue        = (!empty($configuration['logsQueue'])      ? $configuration['logsQueue']       : $this->logsQueue);
        }

        $mongo              = new MongoClient($configuration['host']);
        $this->connection   = $mongo->{$this->db};
    }

    private function __clone(){}
    private function __wakeup(){}

    /**
     * Return settings for queues
     *
     * parameters:
     * name - Name of flag
     * period - Period in seconds
     * min_pause - Min pause between tasks
     * max - Max count of tasks in period
     * recalc_period_pause - Min pause between recalculation (update time for execution of task in period)
     * overwrite_type - 1,2,3 Logic for items in queue
     */
    public function getSettingsQueue($settingsName = ''){
        $items = array(
            'in_minute' => array(
                'name'                  => 'in_minute',
                'period'                => 60,
                'min_pause'             => 0,
                'max'                   => 99999999,
                'recalc_period_pause'   => 0,
                'overwrite_type'        => 3
            ),
            'in_hour' => array(
                'name'                  => 'in_hour',
                'period'                => 3600,
                'min_pause'             => 0,
                'max'                   => 99999999,
                'recalc_period_pause'   => 600,
                'overwrite_type'        => 3
            )
        );

        if( !empty($settingsName) ){
            $items = $items[$settingsName];
        }

        return $items;
    }

    /**
     * Get tasks from waiting queue
     *
     * @param int $from
     * @param int $to
     * @param string $period_name
     * @return array|MongoCursor
     */
    public function getWaitQueue($from, $to, $period_name = ''){
        $where = array();
        $where['exec_time'] = array('$gte' => $from, '$lt' => $to);
        if( !empty($period_name) ){
            $where['flag'] = $period_name;
        }

        $items = $this->connection->{$this->waitQueue}->find($where)->sort(array('exec_time' => 1, 'add_time' => 1));
        $items = $this->toArray($items);

        return $items;
    }

    /**
     * Get tasks from execution queue
     *
     * @param int $from
     * @param int $to
     * @param string $period_name
     * @return array|MongoCursor
     */
    public function getExecutionQueue($from, $to, $period_name = ''){
        $where = array();
        $where['exec_time'] = array('$gte' => $from, '$lt' => $to);
        if( !empty($period_name) ){
            $where['flag'] = $period_name;
        }

        $items = $this->connection->{$this->executionQueue}->find($where)->sort(array('exec_time' => 1, 'add_time' => 1));
        $items = $this->toArray($items);

        return $items;
    }

    /**
     * Get task by id from waiting queue
     * @param string $id
     * @return array|null
     */
    public function getWaitOne($id){
        $id     = new MongoId($id);
        $data   = $this->connection->{$this->waitQueue}->findOne(array("_id" => $id));

        return $data;
    }

    /**
     * Get task by id from execution queue
     * @param string $id
     * @return array|null
     */
    public function getExecutionOne($id){
        $id     = new MongoId($id);
        $data   = $this->connection->{$this->executionQueue}->findOne(array("_id" => $id));

        return $data;
    }

    /**
     * Get task to waiting queue
     *
     * @param array $data
     */
    public function addWaitQueue($data){
        $this->connection->{$this->waitQueue}->insert($data);
    }

    /**
     * Get task to execution queue
     *
     * @param array $data
     */
    public function addExecutionQueue($data){
        $this->connection->{$this->executionQueue}->insert($data);
    }

    /**
     * Remove task from waiting queue
     *
     * @param array $data
     */
    public function removeWaitQueue($data){
        $this->connection->{$this->waitQueue}->remove($data);
    }

    /**
     * Remove task from execution queue
     *
     * @param array $data
     */
    public function removeExecutionQueue($data){
        $this->connection->{$this->executionQueue}->remove($data);
    }

    /**
     * Update task in waiting queue
     *
     * @param array $data
     */
    public function updateWaitQueue($item, $data){
        $this->connection->{$this->waitQueue}->update($item, array('$set' => $data));
    }

    /**
     * Update task in execution queue
     *
     * @param array $data
     */
    public function updateExecutionQueue($item, $data){
        $this->connection->{$this->executionQueue}->update($item, array('$set' => $data));
    }

    /**
     * Add log
     *
     * @param array $data
     */
    public function addLog($operation, $data){
        $data['log_date']       = date('Y-m-d H:i:s');
        $data['log_operation']  = $operation;
        $this->connection->{$this->logsQueue}->insert($data);
    }

    /**
     * Convert MongoCursor to object
     *
     * @param $data
     * @return array
     */
    protected function toArray($data){
        if(!empty($data) ){
            $data = iterator_to_array($data);
            if( !empty($data) ){
                foreach($data as $k_item => $v_item){
                    $data[$k_item]['exec_id'] = (string)$v_item['_id'];
                }
            }
        }

        return $data;
    }

}