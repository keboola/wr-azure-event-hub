<?php

declare(strict_types=1);

namespace Keboola\AzureEventHubWriter;

use React\ChildProcess\Process;
use React\Promise\ExtendedPromiseInterface;
use React\Promise\PromiseInterface;
use React\Stream\ReadableResourceStream;
use React\Stream\WritableResourceStream;

/**
 * Helper class,
 * wraps Process and process Promise together and adds some shortcut methods.
 */
class ProcessWrapper
{
    private Process $process;

    private ExtendedPromiseInterface $promise;

    public function __construct(Process $process, PromiseInterface $promise)
    {
        if (!$promise instanceof ExtendedPromiseInterface) {
            throw new \UnexpectedValueException('Expected ExtendedPromiseInterface.');
        }

        $this->process = $process;
        $this->promise = $promise;
    }

    public function getProcess(): Process
    {
        return $this->process;
    }

    public function getPromise(): ExtendedPromiseInterface
    {
        return $this->promise;
    }

    public function getStdout(): ReadableResourceStream
    {
        /** @var ReadableResourceStream $stdout */
        $stdout = $this->process->stdout;
        return $stdout;
    }

    public function getStderr(): ReadableResourceStream
    {
        /** @var ReadableResourceStream $stderr */
        $stderr = $this->process->stderr;
        return $stderr;
    }

    public function getJsonStream(): ReadableResourceStream
    {
        // We use separated file descriptor for JSON documents stream, see ProcessFactory
        /** @var ReadableResourceStream $jsonStream */
        $jsonStream = $this->process->pipes[ProcessFactory::JSON_STREAM_FD];
        return $jsonStream;
    }
}
