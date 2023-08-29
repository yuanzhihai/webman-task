<?php
declare (strict_types=1);

namespace yzh52521\Task;

use support\Container;
use support\Db;
use support\Redis;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;
use Workerman\Connection\AsyncTcpConnection;
use Workerman\Connection\TcpConnection;
use Workerman\Crontab\Crontab;
use Workerman\Worker;
use yzh52521\Task\Mutex\RedisServerMutex;
use yzh52521\Task\Mutex\RedisTaskMutex;
use yzh52521\Task\Mutex\ServerMutex;
use yzh52521\Task\Mutex\TaskMutex;

/**
 * 注意：定时器开始、暂停、重起
 * Workerman\Crontab 1.0.4 起 立即执行
 */
class Server
{
    const FORBIDDEN_STATUS = '0';

    const NORMAL_STATUS = '1';

    // 命令任务
    public const COMMAND_CRONTAB = '1';
    // 类任务
    public const CLASS_CRONTAB = '2';
    // URL任务
    public const URL_CRONTAB = '3';
    // EVAL 任务
    public const EVAL_CRONTAB = '4';
    //shell 任务
    public const SHELL_CRONTAB = '5';

    private $worker;

    /**
     * @var TaskMutex
     */
    private $taskMutex;

    /**
     * @var ServerMutex
     */
    private $serverMutex;


    /**
     * 调试模式
     * @var bool
     */
    private $debug = false;

    /**
     * 记录日志
     * @var bool
     */
    private $writeLog = false;

    /**
     * 任务进程池
     * @var Crontab[] array
     */
    private $crontabPool = [];

    /**
     * 定时任务表
     * @var string
     */
    private $crontabTable;

    /**
     * 定时任务日志表
     * @var string
     */
    private $crontabLogTable;

    private $tablePrefix = '';

    /**
     * 命令行任务是否后台运行
     * @var bool
     */
    private $runInBackground = false;

    public const WEBMAN_BINARY = 'webman';

    public function __construct()
    {
        $this->delTaskMutex();
    }


    public function onWorkerStart(Worker $worker)
    {
        $config                = config('plugin.yzh52521.task.app.task');
        $this->debug           = $config['debug'] ?? true;
        $this->writeLog        = $config['write_log'] ?? true;
        $this->crontabTable    = $config['crontab_table'];
        $this->crontabLogTable = $config['crontab_table_log'];
        $this->tablePrefix     = $config['prefix'];
        $this->runInBackground = $config['runInBackground'] ?? false;
        $this->worker          = $worker;

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
        $limit = $data['limit'] ?? 15;
        $page  = $data['page'] ?? 1;
        $where = $data['where'] ?? [];
        $data  = Db::table($this->crontabTable)
            ->where($where)
            ->orderBy('id', 'desc')
            ->paginate($limit, '*', 'page', $page);

        return json_encode(['code' => 200, 'msg' => 'ok', 'data' => $data]);
    }

    /**
     * 初始化定时任务
     * @return void
     */
    private function crontabInit(): void
    {
        $ids = Db::table($this->crontabTable)
            ->where('status', self::NORMAL_STATUS)
            ->orderBy('sort', 'desc')
            ->pluck('id')
            ->toArray();
        if (!empty($ids)) {
            foreach ($ids as $id) {
                $this->crontabRun($id);
            }
        }
    }

    /**
     * 创建定时器
     * @param $id
     */
    private function crontabRun($id)
    {
        $data = Db::table($this->crontabTable)
            ->where('id', $id)
            ->where('status', self::NORMAL_STATUS)
            ->first();

        if (!empty($data)) {
            $data = get_object_vars($data);
            switch ($data['type']) {
                case self::COMMAND_CRONTAB:
                    if ($this->decorateRunnable($data)) {
                        $this->crontabPool[$data['id']] = [
                            'id'          => $data['id'],
                            'target'      => $data['target'],
                            'rule'        => $data['rule'],
                            'parameter'   => $data['parameter'],
                            'singleton'   => $data['singleton'],
                            'create_time' => date('Y-m-d H:i:s'),
                            'crontab'     => new Crontab($data['rule'], function () use ($data) {
                                $time      = time();
                                $parameter = $data['parameter'] ?: '';
                                $startTime = microtime(true);
                                $code      = 0;
                                $result    = true;
                                try {
                                    $parameters = !empty($data['parameter']) ? json_decode($data['parameter'], true) : [];
                                    $compiled   = $data['target'];
                                    foreach ($parameters as $key => $value) {
                                        $compiled .= ' ' . escapeshellarg($key);
                                        if ($value !== null) {
                                            $compiled .= ' ' . escapeshellarg($value);
                                        }
                                    }
                                    if ($this->runInBackground) {
                                        // Parentheses are need execute the chain of commands in a subshell
                                        // that can then run in background
                                        $compiled = $compiled . ' > /dev/null 2>&1 &';
                                    }

                                    $command = PHP_BINARY . ' ' . self::WEBMAN_BINARY . ' ' . trim($compiled);
                                    exec($command, $output, $code);
                                    $exception = join(PHP_EOL, $output);
                                } catch (\Throwable $e) {
                                    $result    = false;
                                    $code      = 1;
                                    $exception = $e->getMessage();
                                } finally {
                                    $taskMutex = $this->getTaskMutex();
                                    $taskMutex->remove($data);
                                }

                                $this->debug && $this->writeln('执行定时器任务#' . $data['id'] . ' ' . $data['rule'] . ' ' . $data['target'], $result);

                                $this->isSingleton($data);

                                $endTime = microtime(true);

                                $this->updateRunning($data['id'], $time);

                                $this->writeLog && $this->crontabRunLog([
                                    'crontab_id'   => $data['id'],
                                    'target'       => $data['target'],
                                    'parameter'    => $parameter,
                                    'exception'    => $exception ?? '',
                                    'return_code'  => $code,
                                    'running_time' => round($endTime - $startTime, 6),
                                    'create_time'  => $time,
                                    'update_time'  => $time,
                                ]);
                            })
                        ];
                    }
                    break;
                case self::CLASS_CRONTAB:
                    if ($this->decorateRunnable($data)) {
                        $this->crontabPool[$data['id']] = [
                            'id'          => $data['id'],
                            'target'      => $data['target'],
                            'rule'        => $data['rule'],
                            'parameter'   => $data['parameter'],
                            'singleton'   => $data['singleton'],
                            'create_time' => date('Y-m-d H:i:s'),
                            'crontab'     => new Crontab($data['rule'], function () use ($data) {
                                $time      = time();
                                $class     = trim($data['target']);
                                $startTime = microtime(true);

                                if ($class && strpos($class, '@') !== false) {
                                    $class  = explode('@', $class);
                                    $method = end($class);
                                    array_pop($class);
                                    $class = implode('@', $class);
                                } else {
                                    $method = 'execute';
                                }

                                try {
                                    $code       = 0;
                                    $result     = true;
                                    $parameters = !empty($data['parameter']) ? json_decode($data['parameter'], true) : [];
                                    $this->delivery($class, $method, $parameters);
                                } catch (\Throwable $throwable) {
                                    $result = false;
                                    $code   = 1;
                                } finally {
                                    $taskMutex = $this->getTaskMutex();
                                    $taskMutex->remove($data);
                                }
                                $exception = isset($throwable) ? $throwable->getMessage() : 'ok';

                                $this->debug && $this->writeln('执行定时器任务#' . $data['id'] . ' ' . $data['rule'] . ' ' . $data['target'], $result);

                                $this->isSingleton($data);

                                $endTime = microtime(true);
                                $this->updateRunning($data['id'], $time);

                                $this->writeLog && $this->crontabRunLog([
                                    'crontab_id'   => $data['id'],
                                    'target'       => $data['target'],
                                    'parameter'    => $data['parameter'] ?? '',
                                    'exception'    => $exception ?? '',
                                    'return_code'  => $code,
                                    'running_time' => round($endTime - $startTime, 6),
                                    'create_time'  => $time,
                                    'update_time'  => $time,
                                ]);
                            })
                        ];
                    }
                    break;
                case self::URL_CRONTAB:
                    if ($this->decorateRunnable($data)) {
                        $this->crontabPool[$data['id']] = [
                            'id'          => $data['id'],
                            'target'      => $data['target'],
                            'rule'        => $data['rule'],
                            'parameter'   => $data['parameter'],
                            'singleton'   => $data['singleton'],
                            'create_time' => date('Y-m-d H:i:s'),
                            'crontab'     => new Crontab($data['rule'], function () use ($data) {
                                $time      = time();
                                $url       = trim($data['target']);
                                $startTime = microtime(true);
                                $client    = new \GuzzleHttp\Client();
                                try {
                                    $response = $client->get($url);
                                    $result   = $response->getStatusCode() === 200;
                                    $code     = 0;
                                } catch (\Throwable $throwable) {
                                    $result    = false;
                                    $code      = 1;
                                    $exception = $throwable->getMessage();
                                } finally {
                                    $taskMutex = $this->getTaskMutex();
                                    $taskMutex->remove($data);
                                }

                                $this->debug && $this->writeln('执行定时器任务#' . $data['id'] . ' ' . $data['rule'] . ' ' . $data['target'], $result);

                                $this->isSingleton($data);

                                $endTime = microtime(true);
                                $this->updateRunning($data['id'], $time);

                                $this->writeLog && $this->crontabRunLog([
                                    'crontab_id'   => $data['id'],
                                    'target'       => $data['target'],
                                    'parameter'    => $data['parameter'],
                                    'exception'    => $exception ?? '',
                                    'return_code'  => $code,
                                    'running_time' => round($endTime - $startTime, 6),
                                    'create_time'  => $time,
                                    'update_time'  => $time,
                                ]);

                            })
                        ];
                    }
                    break;
                case self::SHELL_CRONTAB:
                    if ($this->decorateRunnable($data)) {
                        $this->crontabPool[$data['id']] = [
                            'id'          => $data['id'],
                            'target'      => $data['target'],
                            'rule'        => $data['rule'],
                            'parameter'   => $data['parameter'],
                            'singleton'   => $data['singleton'],
                            'create_time' => date('Y-m-d H:i:s'),
                            'crontab'     => new Crontab($data['rule'], function () use ($data) {
                                $time      = time();
                                $parameter = $data['parameter'] ?: '';
                                $startTime = microtime(true);
                                $code      = 0;
                                $result    = true;
                                try {
                                    $exception = shell_exec($data['target']);
                                } catch (\Throwable $e) {
                                    $result    = false;
                                    $code      = 1;
                                    $exception = $e->getMessage();
                                } finally {
                                    $taskMutex = $this->getTaskMutex();
                                    $taskMutex->remove($data);
                                }

                                $this->debug && $this->writeln('执行定时器任务#' . $data['id'] . ' ' . $data['rule'] . ' ' . $data['target'], $result);

                                $this->isSingleton($data);

                                $endTime = microtime(true);
                                $this->updateRunning($data['id'], $time);

                                $this->writeLog && $this->crontabRunLog([
                                    'crontab_id'   => $data['id'],
                                    'target'       => $data['target'],
                                    'parameter'    => $parameter,
                                    'exception'    => $exception,
                                    'return_code'  => $code,
                                    'running_time' => round($endTime - $startTime, 6),
                                    'create_time'  => $time,
                                    'update_time'  => $time,
                                ]);

                            })
                        ];
                    }
                    break;
                case self::EVAL_CRONTAB:
                    if ($this->decorateRunnable($data)) {
                        $this->crontabPool[$data['id']] = [
                            'id'          => $data['id'],
                            'target'      => $data['target'],
                            'rule'        => $data['rule'],
                            'parameter'   => $data['parameter'],
                            'singleton'   => $data['singleton'],
                            'create_time' => date('Y-m-d H:i:s'),
                            'crontab'     => new Crontab($data['rule'], function () use ($data) {
                                $time      = time();
                                $startTime = microtime(true);
                                $result    = true;
                                $code      = 0;
                                try {
                                    eval($data['target']);
                                } catch (\Throwable $throwable) {
                                    $result    = false;
                                    $code      = 1;
                                    $exception = $throwable->getMessage();
                                } finally {
                                    $taskMutex = $this->getTaskMutex();
                                    $taskMutex->remove($data);
                                }

                                $this->debug && $this->writeln('执行定时器任务#' . $data['id'] . ' ' . $data['rule'] . ' ' . $data['target'], $result);

                                $this->isSingleton($data);

                                $endTime = microtime(true);
                                $this->updateRunning($data['id'], $time);

                                $this->writeLog && $this->crontabRunLog([
                                    'crontab_id'   => $data['id'],
                                    'target'       => $data['target'],
                                    'parameter'    => $data['parameter'],
                                    'exception'    => $exception ?? '',
                                    'return_code'  => $code,
                                    'running_time' => round($endTime - $startTime, 6),
                                    'create_time'  => $time,
                                    'update_time'  => $time,
                                ]);
                            })
                        ];
                    }
                    break;
            }
        }
    }

    /**
     * 投递到异步进程
     *
     * @param string $class
     * @param string $method
     * @param array $parameter
     * @return void
     * @throws \Exception
     */
    private function delivery(string $class, string $method, array $parameter): void
    {
        $taskConnection = new AsyncTcpConnection(config('plugin.yzh52521.task.app.task.async_listen'));
        $taskConnection->send(json_encode(['class' => $class, 'method' => $method, 'parameter' => $parameter]));
        $taskConnection->onMessage = function (AsyncTcpConnection $asyncTcpConnection, $taskResult) {
            if ($this->writeLog) {
                echo '异步返回值' . $taskResult . PHP_EOL;
            }
            $asyncTcpConnection->close();
        };
        $taskConnection->connect();
    }

    /**
     * 更新运行次数/时间
     * @param $id
     * @param $time
     * @return void
     */
    private function updateRunning($id, $time)
    {
        Db::update("UPDATE {$this->tablePrefix}{$this->crontabTable} SET running_times = running_times + 1, last_running_time = {$time} WHERE id = {$id}");
    }

    /**
     * 是否单次
     * @param $crontab
     * @return void
     */
    private function isSingleton($crontab)
    {
        if ($crontab['singleton'] == 0 && isset($this->crontabPool[$crontab['id']])) {
            $this->debug && $this->writeln("定时器销毁", true);
            $this->crontabPool[$crontab['id']]['crontab']->destroy();
        }
    }


    /**
     * 解决任务的并发执行问题，任务永远只会同时运行 1 个
     * @param $crontab
     * @return bool
     */
    private function runInSingleton($crontab): bool
    {
        $taskMutex = $this->getTaskMutex();
        if ($taskMutex->exists($crontab) || !$taskMutex->create($crontab)) {
            $this->debug && $this->writeln(sprintf('Crontab task [%s] skipped execution at %s.', $crontab['title'], date('Y-m-d H:i:s')), true);
            return false;
        }
        return true;
    }


    /**
     * 只能一个实例执行
     * @param $crontab
     * @return bool
     */
    private function runOnOneServer($crontab): bool
    {
        $taskMutex = $this->getServerMutex();
        if (!$taskMutex->attempt($crontab)) {
            $this->debug && $this->writeln(sprintf('Crontab task [%s] skipped execution at %s.', $crontab['title'], date('Y-m-d H:i:s')), true);
            return false;
        }
        return true;
    }

    protected function decorateRunnable($crontab): bool
    {
        if ($this->runInSingleton($crontab) && $this->runOnOneServer($crontab)) {
            return true;
        }
        return false;
    }

    private function getTaskMutex(): TaskMutex
    {
        if (!$this->taskMutex) {
            $this->taskMutex = Container::has(TaskMutex::class)
                ? Container::get(TaskMutex::class)
                : Container::get(RedisTaskMutex::class);
        }
        return $this->taskMutex;
    }

    private function getServerMutex(): ServerMutex
    {
        if (!$this->serverMutex) {
            $this->serverMutex = Container::has(ServerMutex::class)
                ? Container::get(ServerMutex::class)
                : Container::get(RedisServerMutex::class);
        }
        return $this->serverMutex;
    }

    /**
     * 记录执行日志
     * @param array $param
     * @return void
     */
    private function crontabRunLog(array $param): void
    {
        Db::table($this->crontabLogTable)->insert($param);
    }

    /**
     * 创建定时任务
     * @param array $param
     * @return string
     */
    private function crontabCreate(array $param): string
    {
        $param['create_time'] = $param['update_time'] = time();
        $id                   = Db::table($this->crontabTable)->insertGetId($param);
        $id && $this->crontabRun($id);

        return json_encode(['code' => 200, 'msg' => 'ok', 'data' => ['code' => (bool)$id]]);
    }

    /**
     * 修改定时器
     * @param array $param
     * @return string
     */
    private function crontabUpdate(array $param): string
    {
        $row = Db::table($this->crontabTable)
            ->where('id', $param['id'])
            ->update($param);

        if (isset($this->crontabPool[$param['id']])) {
            $this->crontabPool[$param['id']]['crontab']->destroy();
            unset($this->crontabPool[$param['id']]);
        }
        if ($param['status'] == self::NORMAL_STATUS) {
            $this->crontabRun($param['id']);
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

            $rows = Db::table($this->crontabTable)
                ->whereIn('id', $ids)
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
            Db::table($this->crontabTable)
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
        $where = $param['where'] ?? [];
        $limit = $param['limit'] ?? 15;
        $page  = $param['page'] ?? 1;
        $param['crontab_id'] && $where[] = ['crontab_id', '=', $param['crontab_id']];

        $data = Db::table($this->crontabLogTable)
            ->where($where)
            ->orderBy('id', 'desc')
            ->paginate($limit, '*', 'page', $page);

        return json_encode(['code' => 200, 'msg' => 'ok', 'data' => $data]);
    }

    /**
     * 输出日志
     * @param $msg
     * @param bool $isSuccess
     */
    private function writeln($msg, bool $isSuccess)
    {
        echo 'worker:' . $this->worker->id . ' [' . date('Y-m-d H:i:s') . '] ' . $msg . ($isSuccess ? " [Ok] " : " [Fail] ") . PHP_EOL;
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
        $tables = Db::select('SHOW TABLES');
        $info   = [];

        foreach ($tables as $key => $val) {
            $info[$key] = current((array)$val);
        }

        return $info;
    }

    /**
     * 删除执行失败的任务key
     * @return void
     */
    private function delTaskMutex()
    {
        $keys = Redis::keys('framework' . DIRECTORY_SEPARATOR . 'crontab-*');
        Redis::del($keys);
    }


    /**
     * 创建定时器任务表
     */
    private function createCrontabTable()
    {
        $sql = <<<SQL
 CREATE TABLE IF NOT EXISTS `{$this->tablePrefix}{$this->crontabTable}`  (
  `id` int(11) UNSIGNED NOT NULL AUTO_INCREMENT,
  `title` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '任务标题',
  `type` tinyint(1) NOT NULL DEFAULT 1 COMMENT '任务类型 (1 command, 2 class, 3 url, 4 eval)',
  `rule` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '任务执行表达式',
  `target` varchar(150) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '调用任务字符串',
  `parameter` varchar(500)  COMMENT '任务调用参数', 
  `running_times` int(11) NOT NULL DEFAULT '0' COMMENT '已运行次数',
  `last_running_time` int(11) NOT NULL DEFAULT '0' COMMENT '上次运行时间',
  `remark` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '备注',
  `sort` int(11) NOT NULL DEFAULT 0 COMMENT '排序，越大越前',
  `status` tinyint(4) NOT NULL DEFAULT 0 COMMENT '任务状态状态[0:禁用;1启用]',
  `create_time` int(11) NOT NULL DEFAULT 0 COMMENT '创建时间',
  `update_time` int(11) NOT NULL DEFAULT 0 COMMENT '更新时间',
  `singleton` tinyint(1) NOT NULL DEFAULT 1 COMMENT '是否单次执行 (0 是 1 不是)',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `title`(`title`) USING BTREE,
  INDEX `create_time`(`create_time`) USING BTREE,
  INDEX `status`(`status`) USING BTREE,
  INDEX `type`(`type`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '定时器任务表' ROW_FORMAT = DYNAMIC
SQL;

        return Db::statement($sql);
    }

    /**
     * 定时器任务流水表
     */
    private function createCrontabLogTable()
    {
        $sql = <<<SQL
CREATE TABLE IF NOT EXISTS `{$this->tablePrefix}{$this->crontabLogTable}`  (
  `id` bigint UNSIGNED NOT NULL AUTO_INCREMENT,
  `crontab_id` bigint UNSIGNED NOT NULL COMMENT '任务id',
  `target` varchar(255) NOT NULL COMMENT '任务调用目标字符串',
  `parameter` varchar(500)  COMMENT '任务调用参数', 
  `exception` text  COMMENT '任务执行或者异常信息输出',
  `return_code` tinyint(1) NOT NULL DEFAULT 0 COMMENT '执行返回状态[0成功; 1失败]',
  `running_time` varchar(10) NOT NULL COMMENT '执行所用时间',
  `create_time` int(11) NOT NULL DEFAULT 0 COMMENT '创建时间',
  `update_time` int(11) NOT NULL DEFAULT 0 COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `create_time`(`create_time`) USING BTREE,
  INDEX `crontab_id`(`crontab_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '定时器任务执行日志表' ROW_FORMAT = DYNAMIC
SQL;

        return Db::statement($sql);
    }

}
