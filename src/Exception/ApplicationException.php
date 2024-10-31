<?php

declare(strict_types=1);

namespace Keboola\AzureEventHubWriter\Exception;

use Keboola\CommonExceptions\ApplicationExceptionInterface;
use RuntimeException;

class ApplicationException extends RuntimeException implements ApplicationExceptionInterface
{

}
