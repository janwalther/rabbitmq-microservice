<?php
namespace janwalther\RabbitmqRPC;

use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Message\AMQPMessage;

abstract class RpcReceiver
{
    /** @var AbstractConnection */
    private $connection;

    public function __construct(AbstractConnection $connection)
    {
        $this->connection = $connection;
    }

    /**
     * Listens for incoming messages
     */
    public function listen()
    {
        $channel = $this->connection->channel();

        $channel->queue_declare(
            'rpc_'.get_class($this),    #queue
            false,          #passive
            false,          #durable
            false,          #exclusive
            false           #autodelete
        );

        $channel->basic_qos(
            null,   #prefetch size
            1,      #prefetch count
            null    #global
        );

        $channel->basic_consume(
            'rpc_queue',                #queue
            '',                         #consumer tag
            false,                      #no local
            false,                      #no ack
            false,                      #exclusive
            false,                      #no wait
            array($this, 'callback')    #callback
        );

        $channel->wait();

        $channel->close();
        $this->connection->close();
    }

    abstract protected function process(string $requestMessage): string;

    /**
     * Executes when a message is received.
     *
     * @param AMQPMessage $requestMessage
     */
    public function callback(AMQPMessage $requestMessage) {

        $result = $this->process($requestMessage->body);

        /*
         * Creating a reply message with the same request id than the incoming message
         */
        $responseMessage = new AMQPMessage(
            $result,
            array('request_id' => $requestMessage->get('request_id'))
        );

        /*
         * Publishing to the same channel from the incoming message
         */
        $requestMessage->delivery_info['channel']->basic_publish(
            $responseMessage,                   #message
            '',                     #exchange
            $requestMessage->get('reply_to')   #routing key
        );

        /*
         * Acknowledging the message
         */
        $requestMessage->delivery_info['channel']->basic_ack(
            $requestMessage->delivery_info['delivery_tag']
        );
    }
}
