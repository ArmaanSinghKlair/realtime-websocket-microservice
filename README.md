# RealTime WebSocket Microservice

A scalable, real-time messaging microservice built with **Java**, **Spring**, **WebSockets**, and **Redis**, designed to power a paper trading platform with real-time trading actions. This microservice features a **custom pub/sub design** for **inter-microservice communication** and uses **Redis Pub/Sub and Streams** for **intra-microservice communication**, enabling fast, reliable messaging for actions like market orders, chat, and user updates. The project showcases my expertise in distributed systems, real-time communication, and full-stack development, making it a compelling portfolio piece.

![image](https://github.com/user-attachments/assets/5453a8dc-0817-447c-991b-8fe6ab88c64e)

## 🚀 Features

- **Real-Time Messaging**: Delivers WebSocket messages with minimal latency for paper trading actions like trade orders and market updates.
- **Custom Pub/Sub for Inter-Microservice Communication**: Built a tailored pub/sub system in Java to route messages between same-server WebSocket connections.
  - Created a **Message Broker** component that handles handing off messages to topics with non-blocking behaviour.
  - Each **Topic** holds an in-memory buffer of messages waiting to be flushed asynchronously to attached **Subscribers**
  - Each **Subscriber** holds an in-memory buffer of messages waiting to be flushed to client via WebSocket connection.
  - Decouple **Publishers** and **Subscribers** and allows for non-blocking and asynchronous processing.
- **Redis for Intra-Microservice Communication**: Each WebSocket messages is classed as high and low priority.
  - **Redis Streams**: High priority messages use Redis Streams because it ensures that messages aren't lost if backend services loses the Redis connection. After the service reconnects, it can still read the messages it missed offline. Examples include user's market orders, connections/disconnections.
  - **Redis Pub/Sub**: Handles low-priority messages, such as mouse pointer position updates for quickly sharing data that system can afford to lose.
- **Scalable Architecture**: We can scale horizontally the number of WebSocket Microservice instances. When a new backend instance spins up, all new WebSocket connections are routed to that instance until the number of connections on it is roughly the same as on all other instances. ie Least Connection Algorithm.

## 🛠️ Tech Stack

- **Backend**: Java 17, Spring Framework
- **Messaging**: WebSockets, Redis Pub/Sub, Redis Streams
- **Concurrency**: @Async, Spring Thread pools
- **Frontend**: React (for the paper trading platform)
- **Deployment**: [Docker](https://github.com/ArmaanSinghKlair/realtime-app-docker-config/tree/main/realtime-app-docker-config/realtime-webapp-combined-image)

## 🏗️ Architecture

The **RealTime WebSocket Microservice** powers real-time messaging for a paper trading platform, enabling users to simulate stock and crypto trading with features like trade execution, market updates, and collaborative interactions. The architecture is built around two key communication strategies:

1. **Inter-Microservice Communication**:
   - A **custom pub/sub system** built in Java routes messages between microservices to coordinate platform actions. For example:
     - When a user places a market order (e.g., buying a stock), the order is sent to a topic (e.g., `trade:stock`) and routed to other microservices for processing.
     - User updates, such as joining a trading session, are broadcast to ensure all microservices stay in sync.
   - Messages are directed to topics based on a topic identifier and delivered to subscribers in real time.

2. **Intra-Microservice Communication**:
   - **Redis Pub/Sub**: Broadcasts low-priority messages within a microservice for real-time, non-critical updates. For example:
     - Mouse pointer position updates are sent to a topic (e.g., `collaboration:chart`) to enable collaborative features, such as shared chart interactions among traders.
   - **Redis Streams**: Provides persistent, reliable delivery for high-priority messages requiring guaranteed delivery. For example:
     - User connections/disconnections (e.g., a trader logging in/out) are stored and delivered to ensure accurate session tracking.
     - Chat messages (e.g., group discussions in a trading room) are persisted for reliability.
     - Individual market orders (e.g., a user’s buy/sell request) are guaranteed to reach subscribers for execution.

3. **Core Components**:
   - **WebSocket Clients**: React app users connect via WebSockets, distributed by a load balancer to handle actions like placing trades or chatting.
   - **Message Router**: Directs messages to topics using the custom pub/sub system and Redis for cross-microservice delivery.
   - **Topics**: Manage message queues and send messages to subscribers using threads from a Spring-configured thread pool. For example, a topic like `market:crypto` delivers real-time price updates to subscribed traders.
   - **Subscribers**: Process messages through their own queues, operating independently to avoid delays, such as delivering a trade confirmation to a user’s React app.
   - **Thread Pool**: A configured thread pool optimizes resources for asynchronous message processing, ensuring smooth handling of high message volumes.

![Architecture Flow](architecture-flow.png) <!-- Placeholder; add a flow diagram -->

## 📈 Scalability & Performance

- **Horizontal Scaling**: Stateless microservices scale by adding instances, with Redis and the custom pub/sub system ensuring messages reach all subscribers.
- **Non-Blocking Design**: Asynchronous processing and bounded queues prevent bottlenecks, even during high trading activity.
- **Optimized Concurrency**: A thread pool sized to CPU cores maximizes throughput with minimal resource use.
- **Reliability**: Redis Streams guarantee delivery for critical messages like market orders, while bounded queues manage memory.

## 📚 Skills Demonstrated
This project highlights my expertise in:
- **Java & Spring**: Building a custom pub/sub system for inter-microservice communication.
- **Distributed Systems**: Designing a multi-microservice architecture with Redis Pub/Sub and Streams for intra-microservice messaging.
- **Real-Time Communication**: Enabling WebSocket messaging with load balancing for trading actions.
- **Concurrency**: Managing threads, atomic operations, and bounded queues for performance.
- **Full-Stack Development**: Integrating a React frontend with a Java backend for a paper trading platform.

## 🌟 Why This Project?
The **RealTime WebSocket Microservice** reflects my passion for building scalable, real-time systems. By creating a custom pub/sub design for inter-microservice communication and leveraging Redis for intra-microservice messaging, this project powers a paper trading platform with real-time trading and collaboration features, showcasing my ability to solve complex challenges in distributed systems and full-stack development.

---

⭐ **Star this repo if you find it useful!** ⭐
