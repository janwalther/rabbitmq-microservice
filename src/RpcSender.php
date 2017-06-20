<?php
namespace janwalther\RabbitmqRPC;

use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

class RpcSender
{
    /** @var AbstractConnection */
    private $connection;
    private $response;

    /**
     * @var string
     */
    private $requestId;

    public function __construct(AbstractConnection $connection)
    {
        $this->connection = $connection;
    }

    public function request(string $serviceClass, string $payload)
    {
        $channel = $this->connection->channel();

        /*
         * creates an anonymous exclusive callback queue
         * $callback_queue has a value like amq.gen-_U0kJVm8helFzQk9P0z9gg
         */
        list($callback_queue, ,) = $channel->queue_declare(
            '', 	#queue
            false, 	#passive
            false, 	#durable
            true, 	#exclusive
            false	#auto delete
        );

        $channel->basic_consume(
            $callback_queue, 			#queue
            '', 						#consumer tag
            false, 						#no local
            false, 						#no ack
            false, 						#exclusive
            false, 						#no wait
            array($this, 'onResponse')	#callback
        );

        $this->response = null;

        /*
         * $this->corr_id has a value like 53e26b393313a
         */
        $this->requestId = uniqid('', false);

        /*
         * create a message with two properties: reply_to, which is set to the
         * callback queue and correlation_id, which is set to a unique value for
         * every request
         */
        $msg = new AMQPMessage(
            $payload,
            array('request_id' => $this->requestId, 'reply_to' => $callback_queue)
        );

        /*
         * The request is sent to an rpc_queue queue.
         */
        $channel->basic_publish(
            $msg,		#message
            '', 		#exchange
            'rpc_'.$serviceClass	#routing key
        );

        while(!$this->response) {
            $channel->wait();
        }

        $channel->close();
        $this->connection->close();

        return $this->response;
    }

    /**
     * When a message appears, it checks the correlation_id property. If it
     * matches the value from the request it returns the response to the
     * application.
     *
     * @param AMQPMessage $response
     */
    public function onResponse(AMQPMessage $response) {
        if($response->get('request_id') === $this->requestId) {
            $this->response = $response->body;
        }
    }
}
