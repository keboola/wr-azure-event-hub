<?php

declare(strict_types=1);

namespace Keboola\AzureEventHubWriter\MessageMapper;

use Iterator;

interface MessageMapper
{
    public function getMessages(): Iterator;
}
