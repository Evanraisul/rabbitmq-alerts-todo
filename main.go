package main

import (
	"context"
	"fmt"
	"log"

	"github.com/michaelklishin/rabbit-hole/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/yaml"
	corev1 "k8s.io/api/core/v1"
)

func main() {

	client, err := rabbithole.NewClient("http://localhost:15672", "guest", "guest")
	if err != nil {
		log.Fatalf("Failed to create RabbitMQ client: %v", err)
	}

	// --- 2. UnRoutableMessages: ---
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
	fmt.Println()

	// --- 3. HighConnectionChurn: ---
	// Fetch all active connections
	connections, err := client.ListConnections()
	if err != nil {
		log.Fatalf("Failed to fetch connections: %v", err)
	}

	// Print the total number of connections
	fmt.Printf("Total Active Connections: %d\n\n", len(connections))

	// Print details of each connection
	fmt.Println("Active Connections:")
	for _, conn := range connections {
		fmt.Printf("Connection Name: %s\n", conn.Name)
		fmt.Printf("  Host: %s\n", conn.Host)
		fmt.Printf("  User: %s\n", conn.User)
		fmt.Printf("  State: %s\n", conn.State)
		fmt.Printf("  Channels: %d\n", conn.Channels)
		fmt.Printf("  Connected At: %s\n", conn.ConnectedAt)
		fmt.Printf("  Client Properties: %+v\n", conn.ClientProperties)
	}
	fmt.Println()

	// ----4. LowDiskWatermarkPredicted:-----
	// Queues Backlogs
	queues, err = client.ListQueuesIn(vhost)
	if err != nil {
		log.Fatalf("Failed to fetch queues: %v", err)
	}

	fmt.Println("\n--- Queues Backlogs ---")
	for _, queue := range queues {
		fmt.Printf("Queue: %s\n", queue.Name)
		fmt.Printf("  Messages Ready: %d\n", queue.MessagesReady)
		fmt.Printf("  Messages Unacknowledged: %d\n", queue.MessagesUnacknowledged)
		fmt.Printf("  Total Messages: %d\n", queue.Messages)
	}
	fmt.Println()

	// Disk per Node
	nodeInfo, err := client.ListNodes()
	if err != nil {
		log.Fatalf("Failed to fetch node information: %v", err)
	}

	fmt.Println("\n--- Disk Usage Information ---")
	for _, node := range nodeInfo {
		fmt.Printf("Node: %s\n", node.Name)

		fmt.Printf("  Disk Free Limit: %.2f MB\n", float64(node.DiskFreeLimit)/1024/1024)
		fmt.Printf("  Disk Free: %.2f MB\n", float64(node.DiskFree)/1024/1024)

		fmt.Printf("  Disk Memory Limit: %d\n", node.MemLimit)
		fmt.Printf("  Disk Memory Used: %d\n", node.MemUsed)
	}
	fmt.Println()

	// Consumer Counts per Queue
	fmt.Println("\n--- Consumer Counts per Queue ---")
	for _, queue := range queues {
		fmt.Printf("Queue: %s\n", queue.Name)
		fmt.Printf("  Consumers: %d\n", queue.Consumers)
		fmt.Printf("  Messages Ready: %d\n", queue.MessagesReady)
		fmt.Printf("  Messages Unacknowledged: %d\n", queue.MessagesUnacknowledged)
	}
	fmt.Println()

	// --- 5. FileDescriptorsNearLimit ---
	// Show active Connection
	connections, err = client.ListConnections()
	if err != nil {
		log.Fatalf("Failed to fetch connections: %v", err)
	}

	fmt.Println("\n--- Active Connections ---")
	fmt.Printf("Total Active Connections: %d\n", len(connections))
	for _, conn := range connections {
		fmt.Printf("Connection Name: %s, User: %s, Channels: %d\n", conn.Name, conn.User, conn.Channels)
	}
	fmt.Println()

	// Durable Queues

	fmt.Println("\n--- Durable Queues ---")
	for _, queue := range queues {
		if queue.Durable {
			fmt.Printf("Queue: %s, Messages: %d, Consumers: %d\n", queue.Name, queue.Messages, queue.Consumers)
		}
	}
	fmt.Println()

	// File Descriptors Usage

	fmt.Println("\n--- File Descriptor Usage ---")
	for _, node := range nodeInfo {
		fmt.Printf("Node: %s\n", node.Name)
		fmt.Printf("  File Descriptors Used: %d\n", node.FdUsed)
		fmt.Printf("  File Descriptors Available: %d\n", node.FdTotal)
		fmt.Printf("  File Descriptor Usage: %.2f%%\n", 100*float64(node.FdUsed)/float64(node.FdTotal))
	}
	fmt.Println()

	// 6. TCPSocketsNearLimit:
	// Connection & Channels, Connection Pooling
	fmt.Println("--- Active Connections and Channels ---")
	for _, conn := range connections {
		fmt.Printf("Connection Name: %s\n", conn.Name)
		fmt.Printf("  User: %s\n", conn.User)
		fmt.Printf("  Host: %s\n", conn.Host)
		fmt.Printf("  State: %s\n", conn.State)
		fmt.Printf("  Channels: %d\n", conn.Channels)
		fmt.Printf("  Connected At: %s\n", conn.ConnectedAt)
	}
	fmt.Println()

	// Fetch all active channels
	channels, err := client.ListChannels()
	if err != nil {
		log.Fatalf("Failed to fetch channels: %v", err)
	}

	fmt.Println("\n--- Active Channels ---")
	for _, channel := range channels {
		fmt.Printf("Channel Name: %s\n", channel.Name)
		fmt.Printf("  Connection: %s\n", channel.ConnectionDetails.Name)
		fmt.Printf("  Consumer Count: %d\n", channel.ConsumerCount)
		fmt.Printf("  Messages Unacknowledged: %d\n", channel.UnacknowledgedMessageCount)
	}
	fmt.Println()

	// Client Connection Management
	fmt.Println("\n--- Active Connections ---")
	fmt.Printf("Total Active Connections: %d\n", len(connections))
	for _, conn := range connections {
		fmt.Printf("Connection Name: %s, User: %s, Channels: %d\n", conn.Name, conn.User, conn.Channels)
	}
	fmt.Println()

	// 7. RabbitMQDown
	// Describe RabbitMQ CR
	// Load kubeconfig
	kubeconfig := "/home/evan/.kube/config" // Replace with your kubeconfig path
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		log.Fatalf("Failed to load kubeconfig: %v", err)
	}

	// Create Kubernetes client
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("Failed to create Kubernetes client: %v", err)
	}

	// Define namespace and RabbitMQ CR name
	namespace := "demo"
	rabbitmqCRName := "rm-quickstart"

	// Get the RabbitMQ CR (assumes CRD is registered in the API server)
	cr, err := clientset.RESTClient().Get().
		AbsPath(fmt.Sprintf("/apis/kubedb.com/v1alpha2/namespaces/%s/rabbitmqs/%s", namespace, rabbitmqCRName)).
		DoRaw(context.TODO())
	if err != nil {
		log.Fatalf("Failed to describe RabbitMQ CR: %v", err)
	}

	// Converting to YAML
	crYaml, err := yaml.JSONToYAML(cr)
	if err != nil {
		log.Fatalf("Error converting JSON to YAML: %v", err)
	}

	// Print the YAML output
	fmt.Println(string(crYaml))
	fmt.Println()

	// Restart RabbitMQ Pods
	// Define RabbitMQ pod label selector
	labelSelector := "app.kubernetes.io/name=rabbitmq" // Adjust label as needed

	// List RabbitMQ pods
	pods, err := clientset.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		log.Fatalf("Failed to list RabbitMQ pods: %v", err)
	}

	// Delete each RabbitMQ pod to restart it
	for _, pod := range pods.Items {
		fmt.Printf("Restarting pod: %s\n", pod.Name)
		err := clientset.CoreV1().Pods(namespace).Delete(context.TODO(), pod.Name, metav1.DeleteOptions{})
		if err != nil {
			log.Printf("Failed to delete pod %s: %v", pod.Name, err)
		} else {
			fmt.Printf("Successfully deleted pod: %s\n", pod.Name)
		}
	}
	fmt.Println()

	//RabbitMQ Pods log
	// Define namespace and RabbitMQ pod name
	podName := "rm-quickstart-0" // Replace with your RabbitMQ pod name

	// Fetch logs for the RabbitMQ pod
	for _, pod := range pods.Items {
		logs, err := clientset.CoreV1().Pods(namespace).GetLogs(pod.Name, &corev1.PodLogOptions{}).DoRaw(context.TODO())
		if err != nil {
			log.Fatalf("Failed to fetch logs for pod %s: %v", podName, err)
		}
		fmt.Printf("Logs for pod %s:\n%s\n", podName, string(logs))
	}
	fmt.Println()
}
