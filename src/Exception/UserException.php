<?php

declare(strict_types=1);

namespace Keboola\AzureEventHubWriter\Exception;

use Exception;
use Keboola\CommonExceptions\UserExceptionInterface;

class UserException extends Exception implements UserExceptionInterface
{

}
