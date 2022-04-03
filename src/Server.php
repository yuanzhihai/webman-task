<?php
declare (strict_types=1);

namespace yzh52521\Task;

use Workerman\Connection\TcpConnection;
use Workerman\Crontab\Crontab;
use Workerman\Worker;

class Server
{
    const NORMAL_STATUS = 1;

    private Worker $worker;

    /**
     * 数据库进程池
     * @var array
     */
    private array $db = [];

    /**
     * 调试模式
     * @var bool
     */
    private bool $debug = false;

    /**
     * 任务进程池
     * @var Crontab[] array
     */
    private array $crontabPool = [];

    /**
     * 定时任务表
     * @var string
     */
    private string $crontabTable;

    /**
     * 定时任务日志表
     * @var string
     */
    private string $crontabLogTable;

    public function __construct()
    {
    }


    public function onWorkerStart($worker)
    {
        $config                = config('plugin.yzh52521.task.app.task');
        $this->debug           = $config['debug'];
        $this->crontabTable    = $config['crontab_table'];
        $this->crontabLogTable = $config['crontab_table_log'];
        $this->worker          = $worker;
        $this->db[$worker->id] = \think\facade\Db::class;

        $this->checkCrontabTables();
        $this->crontabInit();
    }

    /**
     * 当客户端与Workman建立连接时(TCP三次握手完成后)触发的回调函数
     * 每个连接只会触发一次onConnect回调
     * 此时客户端还没有发来任何数据
     * 由于udp是无连接的，所以当使用udp时不会触发onConnect回调，也不会触发onClose回调
     * @param TcpConnection $connection
     */
    public function onConnect(TcpConnection $connection)
    {
        $this->checkCrontabTables();
    }


    public function onMessage(TcpConnection $connection, $data)
    {
        $data   = json_decode($data, true);
        $method = $data['method'];
        $args   = $data['args'];
        $connection->send(call_user_func([$this, $method], $args));
    }


    /**
     * 定时器列表
     * @param array $data
     * @return false|string
     */
    private function crontabIndex(array $data)
    {
        $where = [];
        $limit = $data['limit'] ?? 15;
        $page  = $data['page'] ?? 1;
        $data  = $this->db[$this->worker->id]::table($this->crontabTable)
            ->where($where)
            ->order('id', 'desc')
            ->paginate(($page - 1) * $limit);

        return json_encode(['code' => 200, 'msg' => 'ok', 'data' => $data]);
    }

    /**
     * 初始化定时任务
     * @return void
     */
    private function crontabInit(): void
    {
        $ids = $this->db[$this->worker->id]::table($this->crontabTable)
            ->where('status', self::NORMAL_STATUS)
            ->order('sort', 'desc')
            ->column('id');
        if (!empty($ids)) {
            foreach ($ids as $id) {
                $this->crontabRun($id);
            }
        }
    }

    /**
     * 创建定时器
     * @param int $id
     */
    private function crontabRun(int $id)
    {
        $data = $this->db[$this->worker->id]::table($this->crontabTable)
            ->where('id', $id)
            ->where('status', self::NORMAL_STATUS)
            ->find();

        if (!empty($data)) {
            $this->crontabPool[$data['id']] = [
                'id'          => $data['id'],
                'shell'       => $data['shell'],
                'frequency'   => $data['frequency'],
                'remark'      => $data['remark'],
                'create_time' => date('Y-m-d H:i:s'),
                'crontab'     => new Crontab($data['frequency'], function () use ($data) {
                    $time  = time();
                    $shell = trim($data['shell']);
                    $this->debug && $this->writeln('执行定时器任务#' . $data['id'] . ' ' . $data['frequency'] . ' ' . $shell);
                    $startTime = microtime(true);
                    exec($shell, $output, $code);
                    $endTime = microtime(true);
                    $this->db[$this->worker->id]::query("UPDATE {$this->crontabTable} SET running_times = running_times + 1, last_running_time = {$time} WHERE id = {$data['id']}");
                    $this->crontabRunLog([
                        'sid'          => $data['id'],
                        'command'      => $shell,
                        'output'       => join(PHP_EOL, $output),
                        'return_code'  => $code,
                        'running_time' => round($endTime - $startTime, 6),
                        'create_time'  => $time,
                        'update_time'  => $time,
                    ]);

                })
            ];
        }
    }

    /**
     * 记录执行日志
     * @param array $param
     * @return void
     */
    private function crontabRunLog(array $param): void
    {
        $this->db[$this->worker->id]::table($this->crontabLogTable)->insert($param);
    }

    /**
     * 创建定时任务
     * @param array $param
     * @return string
     */
    private function crontabCreate(array $param): string
    {
        $param['create_time'] = $param['update_time'] = time();
        $id                   = $this->db[$this->worker->id]::table($this->crontabTable)
            ->insertGetId($param);
        $id && $this->crontabRun((int)$id);

        return json_encode(['code' => 200, 'msg' => 'ok', 'data' => ['code' => (bool)$id]]);
    }

    /**
     * 修改定时器
     * @param array $param
     * @return string
     */
    private function crontabUpdate(array $param): string
    {
        $row = $this->db[$this->worker->id]::table($this->crontabTable)
            ->where('id', $param['id'])
            ->update($param);

        if ($param['status'] == self::NORMAL_STATUS) {
            $this->crontabRun($param['id']);
        } else {
            if (isset($this->crontabPool[$param['id']])) {
                $this->crontabPool[$param['id']]['crontab']->destroy();
                unset($this->crontabPool[$param['id']]);
            }
        }

        return json_encode(['code' => 200, 'msg' => 'ok', 'data' => ['code' => (bool)$row]]);

    }


    /**
     * 清除定时任务
     * @param array $param
     * @return string
     */
    private function crontabDelete(array $param): string
    {
        if ($id = $param['id']) {
            $ids = explode(',', (string)$id);

            foreach ($ids as $item) {
                if (isset($this->crontabPool[$item])) {
                    $this->crontabPool[$item]['crontab']->destroy();
                    unset($this->crontabPool[$item]);
                }
            }

            $rows = $this->db[$this->worker->id]::table($this->crontabTable)
                ->where('id in (' . $id . ')')
                ->delete();

            return json_encode(['code' => 200, 'msg' => 'ok', 'data' => ['code' => (bool)$rows]]);
        }

        return json_encode(['code' => 200, 'msg' => 'ok', 'data' => ['code' => true]]);
    }

    /**
     * 重启定时任务
     * @param array $param
     * @return string
     */
    private function crontabReload(array $param): string
    {
        $ids = explode(',', (string)$param['id']);

        foreach ($ids as $id) {
            if (isset($this->crontabPool[$id])) {
                $this->crontabPool[$id]['crontab']->destroy();
                unset($this->crontabPool[$id]);
            }
            $this->db[$this->worker->id]::table($this->crontabTable)
                ->where('id', $id)
                ->update(['status' => self::NORMAL_STATUS]);
            $this->crontabRun($id);
        }

        return json_encode(['code' => 200, 'msg' => 'ok', 'data' => ['code' => true]]);
    }


    /**
     * 执行日志列表
     * @param array $param
     * @return string
     */
    private function crontabLog(array $param): string
    {
        $where = [];
        $limit = $param['limit'] ?? 15;
        $page  = $param['page'] ?? 1;
        $param['sid'] && $where[] = ['sid', '=', $param['sid']];

        $data = $this->db[$this->worker->id]
            ->table($this->crontabLogTable)
            ->where($where)
            ->order('id', 'desc')
            ->paginate(($page - 1) * $limit);

        return json(['code' => 200, 'msg' => 'ok', 'data' => $data]);
    }

    /**
     * 输出日志
     * @param $msg
     */
    private function writeln($msg)
    {
        echo '[' . date('Y-m-d H:i:s') . '] ' . $msg . "   [Ok] " . PHP_EOL;
    }

    /**
     * 检测表是否存在
     */
    private function checkCrontabTables()
    {
        $allTables = $this->getDbTables();
        !in_array($this->crontabTable, $allTables) && $this->createCrontabTable();
        !in_array($this->crontabLogTable, $allTables) && $this->createCrontabLogTable();
    }

    /**
     * 获取数据库表名
     * @return array
     */
    private function getDbTables(): array
    {
        return $this->db[$this->worker->id]::getTables();
    }

    /**
     * 创建定时器任务表
     */
    private function createCrontabTable()
    {
        $sql = <<<SQL
 CREATE TABLE IF NOT EXISTS `system_crontab`  (
  `id` int(11) UNSIGNED NOT NULL AUTO_INCREMENT,
  `title` varchar(60) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '任务标题',
  `frequency` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '任务频率',
  `shell` varchar(150) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '任务脚本',
  `running_times` int(11) NOT NULL DEFAULT '0' COMMENT '已运行次数',
  `last_running_time` int(11) NOT NULL DEFAULT '0' COMMENT '最近运行时间',
  `remark` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '任务备注',
  `sort` int(11) NOT NULL DEFAULT 0 COMMENT '排序，越大越前',
  `status` tinyint(4) NOT NULL DEFAULT 0 COMMENT '任务状态状态[0:禁用;1启用]',
  `create_time` int(11) NOT NULL DEFAULT 0 COMMENT '创建时间',
  `update_time` int(11) NOT NULL DEFAULT 0 COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `title`(`title`) USING BTREE,
  INDEX `create_time`(`create_time`) USING BTREE,
  INDEX `status`(`status`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '定时器任务表' ROW_FORMAT = DYNAMIC
SQL;

        return $this->db[$this->worker->id]::query($sql);
    }

    /**
     * 定时器任务流水表
     */
    private function createCrontabLogTable()
    {
        $sql = <<<SQL
CREATE TABLE IF NOT EXISTS `system_crontab_log`  (
  `id` int(11) UNSIGNED NOT NULL AUTO_INCREMENT,
  `sid` int(60) NOT NULL COMMENT '任务id',
  `command` varchar(255) NOT NULL COMMENT '执行命令',
  `output` text NOT NULL COMMENT '执行输出',
  `return_code` tinyint(1) NOT NULL DEFAULT 0 COMMENT '执行返回状态[0成功; 1失败]',
  `running_time` varchar(10) NOT NULL COMMENT '执行所用时间',
  `create_time` int(11) NOT NULL DEFAULT 0 COMMENT '创建时间',
  `update_time` int(11) NOT NULL DEFAULT 0 COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `create_time`(`create_time`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '定时器任务流水表' ROW_FORMAT = DYNAMIC
SQL;

        return $this->db[$this->worker->id]::query($sql);
    }

}