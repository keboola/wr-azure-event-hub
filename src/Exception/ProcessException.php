<?php

declare(strict_types=1);

namespace Keboola\AzureEventHubWriter\Exception;

use Throwable;

class ProcessException extends ApplicationException
{
    public function __construct(string $message, int $exitCode, ?Throwable $previous = null)
    {
        parent::__construct($message, $exitCode, $previous);
    }

    public function getExitCode(): int
    {
        return $this->code;
    }
}
