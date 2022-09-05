<?php

declare(strict_types=1);

namespace yzh52521\Task\Mutex;

use support\Redis;
use think\helper\Arr;
use yzh52521\Task\util\MacAddress;

class RedisServerMutex implements ServerMutex
{
    protected $mutexExpires = 3600;
    /**
     * @var Redis
     */
    private $redisFactory;

    /**
     * @var null|string
     */
    private $macAddress;

    public function __construct(Redis $redisFactory)
    {
        $this->redisFactory = $redisFactory;

        $this->macAddress = $this->getMacAddress();
    }

    private function getMutexExpires()
    {
        return $this->mutexExpires;
    }


    /**
     *  Attempt to obtain a server mutex for the given crontab.
     */
    public function attempt($crontab): bool
    {
        $result = (bool)$this->redisFactory::set(
            $this->getMutexName($crontab),
            $this->macAddress, 'EX', $this->getMutexExpires(), 'NX'
        );

        if ($result === true) {
            return true;
        }
        return $this->redisFactory::get($this->getMutexName($crontab)) === $this->macAddress;
    }

    /**
     * Get the task mutex for the given crontab.
     */
    public function get($crontab): string
    {
        return (string)$this->redisFactory::get(
            $this->getMutexName($crontab)
        );
    }

    protected function getMutexName($crontab): string
    {
        return 'webman' . DIRECTORY_SEPARATOR . 'crontab-' . sha1($crontab['title'] . $crontab['rule']) . '-sv';
    }


    protected function getMacAddress(): ?string
    {
        $macAddresses = (new MacAddress())->Local_Mac_Address();
        foreach (Arr::wrap($macAddresses) as $name => $address) {
            if ($address && $address !== '00:00:00:00:00:00') {
                return $name . ':' . str_replace(':', '', $address);
            }
        }

        return null;
    }
}
