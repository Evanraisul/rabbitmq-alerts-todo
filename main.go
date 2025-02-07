package main

import (
	"fmt"
	"log"

	"github.com/michaelklishin/rabbit-hole/v2"
)

func main() {

	client, err := rabbithole.NewClient("http://localhost:15672", "guest", "guest")
	if err != nil {
		log.Fatalf("Failed to create RabbitMQ client: %v", err)
	}

	// Fetch all bindings
	bindings, err := client.ListBindings()
	if err != nil {
		log.Fatalf("Failed to fetch bindings: %v", err)
	}

	// Binding details...
	fmt.Println("All Bindings List:")
	for _, binding := range bindings {
		// Skip internal or unused bindings
		/*if binding.Source == "" || binding.Destination == "" {
			continue
		}*/
		fmt.Printf("Virtual Host: %s, Source: %s, Destination: %s, Routing Key: %s, PropertiesKey: %s, Arguments: %s\n",
			binding.Vhost, binding.Source, binding.Destination, binding.RoutingKey, binding.PropertiesKey, binding.Arguments)
	}
	fmt.Println()

	// Routing Rules for specific virtual host, Exchange or Queue
	// Specific Virtual Host
	vhost := "/"
	bindings, err = client.ListBindingsIn(vhost)
	if err != nil {
		log.Fatalf("Failed to fetch bindings for virtual host %s: %v", vhost, err)
	}

	// Print the bindings
	fmt.Printf("Bindings for virtual host '%s':\n", vhost)
	for _, binding := range bindings {
		fmt.Printf("Virtual Host: %s, Destination: %s, Routing Key: %s, PropertiesKey: %s, Arguments: %s\n",
			binding.Vhost, binding.Destination, binding.RoutingKey, binding.PropertiesKey, binding.Arguments)
	}
	fmt.Println()

	// Specific Exchange
	exchangeName := "testExchange"
	bindings, err = client.ListExchangeBindings(vhost, exchangeName, rabbithole.BindingSource)
	if err != nil {
		log.Fatalf("Failed to fetch bindings for exchange %s: %v", exchangeName, err)
	}

	// Print the bindings
	fmt.Printf("Bindings for Exchange '%s':\n", exchangeName)
	for _, binding := range bindings {
		fmt.Printf("Virtual Host: %s, Destination: %s, Routing Key: %s, PropertiesKey: %s, Arguments: %s\n",
			binding.Vhost, binding.Destination, binding.RoutingKey, binding.PropertiesKey, binding.Arguments)
	}
	fmt.Println()

	// Specific Queue
	queueName := "testQueue"
	bindings, err = client.ListQueueBindings(vhost, queueName)
	if err != nil {
		log.Fatalf("Failed to fetch bindings for queue %s: %v", queueName, err)
	}

	// Print the bindings
	fmt.Printf("Bindings for Queue '%s':\n", queueName)
	for _, binding := range bindings {
		fmt.Printf("Virtual Host: %s, Destination: %s, Routing Key: %s, PropertiesKey: %s, Arguments: %s\n",
			binding.Vhost, binding.Destination, binding.RoutingKey, binding.PropertiesKey, binding.Arguments)
	}
	fmt.Println()

	// Monitor Dead Letter Exchange
	// Fetch all queues in the virtual host
	queues, err := client.ListQueuesIn(vhost)
	if err != nil {
		log.Fatalf("Failed to fetch queues: %v", err)
	}

	fmt.Println("Current Condition of Queues (with or without Dead Letter Exchange (DLX)):")
	for _, queue := range queues {
		// Check if the queue has a dead letter exchange configured
		dlx, dlxExists := queue.Arguments["x-dead-letter-exchange"]
		if dlxExists {
			fmt.Printf("Queue: %s\n", queue.Name)
			fmt.Printf("  Dead Letter Exchange: %v\n", dlx)

			// Optionally, check the dead letter routing key
			dlRoutingKey, dlRoutingKeyExists := queue.Arguments["x-dead-letter-routing-key"]
			if dlRoutingKeyExists {
				fmt.Printf("  Dead Letter Routing Key: %v\n", dlRoutingKey)
			} else {
				fmt.Println("  Dead Letter Routing Key: (not set)")
			}
			// Check the message count in the DLQ
			dlqName := fmt.Sprintf("dlq.%s", queue.Name) // Example naming convention
			dlq, err := client.GetQueue(vhost, dlqName)
			if err == nil && dlq.Messages > 0 {
				fmt.Printf("  Dead Letter Queue (%s): %d messages\n", dlqName, dlq.Messages)
			} else {
				fmt.Printf("  Dead Letter Queue (%s): No messages\n", dlqName)
			}
		} else {
			fmt.Printf("Queue %s: without DLX\n", queue.Name)
		}
	}
}
