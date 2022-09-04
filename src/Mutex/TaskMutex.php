<?php

declare(strict_types=1);

namespace yzh52521\Task\Mutex;


interface TaskMutex
{
    /**
     * Attempt to obtain a task mutex for the given crontab.
     */
    public function create( $crontab): bool;

    /**
     * Determine if a task mutex exists for the given crontab.
     */
    public function exists($crontab): bool;

    /**
     * Clear the task mutex for the given crontab.
     */
    public function remove($crontab);
}
