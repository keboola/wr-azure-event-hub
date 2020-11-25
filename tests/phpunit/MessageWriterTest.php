<?php

declare(strict_types=1);

namespace Keboola\AzureEventHubWriter\Tests;

use Keboola\AzureEventHubWriter\MessageWriter;
use PHPUnit\Framework\Assert;
use React\Stream\ThroughStream;

class MessageWriterTest extends AbstractTestCase
{
    /**
     * @dataProvider getData
     * @param mixed $input
     * @param string $expectedOutput
     */
    public function testMessageWriter($input, string $expectedOutput): void
    {
        $output = '';
        $stream = new ThroughStream();
        $stream->on('data', function (string $data) use (&$output): void {
            $output .= $data;
        });
        $messageWriter = new MessageWriter($stream);
        $messageWriter->writeMessage($input);
        Assert::assertSame($expectedOutput, $output);
    }

    public function getData(): iterable
    {
        yield 'string' => [
            'test " string',
            '"test \" string"' . MessageWriter::DELIMITER,
        ];

        yield 'array' => [
            ['a', 'b', 'c'],
            '["a","b","c"]' . MessageWriter::DELIMITER,
        ];

        yield 'object' => [
            ['a' => 'a1', 'b' => 'b1', 'c' => 'c1'],
            '{"a":"a1","b":"b1","c":"c1"}' . MessageWriter::DELIMITER,
        ];
    }
}
