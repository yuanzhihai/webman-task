<?php

declare(strict_types=1);

namespace yzh52521\Task\Mutex;

use support\Redis;

class RedisTaskMutex implements TaskMutex
{
    protected $mutexExpires = 3600;
    /**
     * @var Redis
     */
    private $redisFactory;

    public function __construct(Redis $redisFactory)
    {
        $this->redisFactory = $redisFactory;
    }

    private function getMutexExpires()
    {
        return $this->mutexExpires;
    }

    /**
     * Attempt to obtain a task mutex for the given crontab.
     */
    public function create($crontab): bool
    {
        return (bool)$this->redisFactory::set(
            $this->getMutexName($crontab),
            $crontab['title'], 'EX', $this->getMutexExpires(),'NX'
        );
    }

    /**
     * Determine if a task mutex exists for the given crontab.
     */
    public function exists($crontab): bool
    {
        return (bool)$this->redisFactory::exists(
            $this->getMutexName($crontab)
        );
    }

    /**
     * Clear the task mutex for the given crontab.
     */
    public function remove($crontab)
    {
        $this->redisFactory::del(
            $this->getMutexName($crontab)
        );
    }

    protected function getMutexName($crontab): string
    {
        return 'framework' . DIRECTORY_SEPARATOR . 'crontab-' . sha1($crontab['title'] . $crontab['rule']);
    }
}
