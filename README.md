# RealTime WebSocket Microservice

A scalable, real-time messaging microservice built with **Java**, **Spring**, **WebSockets**, and **Redis**, designed to power a [🔗 Paper Trading Platform - Traderjam.online](https://github.com/ArmaanSinghKlair/realtime_paper_trading_multiplayer) with real-time trading actions. This microservice features a **custom pub/sub design** for **inter-microservice communication** and uses **Redis Pub/Sub and Streams** for **intra-microservice communication**, enabling fast, reliable messaging for actions like market orders, chat, and user updates. The project showcases my expertise in distributed systems, real-time communication, and full-stack development, making it a compelling portfolio piece.

<div align="center"><b>Part of a larger system architecture</b> (See red box below)</div>
<hr/>

![image](https://github.com/user-attachments/assets/fc7210f0-82cd-4cbd-b99f-489144375f42)

## 🛠️ Tech Stack

- **Backend**: Java 17, Spring Framework
- **Messaging**: WebSockets, Redis Pub/Sub, Redis Streams
- **Concurrency**: @Async, Spring Thread pools
- **Frontend**: React (for the paper trading platform)
- **Deployment**: [Docker](https://github.com/ArmaanSinghKlair/realtime-app-docker-config/tree/main/realtime-app-docker-config/realtime-webapp-combined-image)

## 🏗️ Architecture

The **RealTime WebSocket Microservice** powers real-time messaging for a paper trading platform. It uses the Pub-Sub pattern for sending messages between websocket clients on the same server and inter servers.

1. **Same-server Communication**:
   - Built a **Custom Pub/Sub System in Java** to route messages between same-server WebSocket connections.
      - Core components: **Message Broker, Publisher, Subscriber, Channels/Topics**.
      - **Message Broker** is an intermediary that routes messages between **Publisher** WebSockets and **Subscriber** WebSockets with non-blocking behaviour ie WebSockets broadcasting a message in trading room are NOT blocked/waiting for other WebSockets recieve the message.
      - **Publishers** send messages to a channel/topic and they are routes to all **Subscribers** subscribed to that channel/topic.
      - It **Decouples** Publishers and Subscribers to enable non-blocking and asynchronous processing.

2. **Inter-Microservice Communication**: Each WebSocket messages is classed as high and low priority.
   - **Redis Streams**: High priority messages use Redis Streams because it ensures that messages aren't lost if backend services loses the Redis connection. After the service reconnects, it can still read the messages it missed offline. For example:
     - User connections/disconnections (e.g., a trader logging in/out) are stored and delivered to ensure accurate session tracking.
     - Chat messages (e.g., group discussions in a trading room) are persisted for reliability.
     - Individual market orders (e.g., a user’s buy/sell request) are guaranteed to reach subscribers for execution.
  - **Redis Pub/Sub** (Less reliable): Handles low-priority messages, eg mouse pointer positions for realtime-updates that system can afford to lose. For example:

3. 📈 Scalability & Performance
    - **Horizontal Scaling**: We can scale horizontally the number of WebSocket Microservice instances. When a new backend instance spins up, all new WebSocket connections are routed to that instance until the number of connections on it is roughly the same as on all other instances. ie Least Connection Algorithm. Custom Pub/Sub and Redis ensure messages are delivered across all microservices consistently.
    - **Non-Blocking Pub/Sub Design**: In my custom Pub/Sub impl, Message Broker sends messages to a topic which stores them in a temporary buffer. The Topic then does asynchronous processing using a Thread Pool to prevent bottlenecks, even during high trading activity.
    - **Reliability**: Redis Streams guarantee delivery for critical messages like market orders, user connection/disconnections.
  
## 🚀 Features

- **Real-Time Messaging**: Delivers WebSocket messages with minimal latency for paper trading actions like trade orders and market updates.
- **Custom Pub/Sub for Inter-Microservice Communication**: Built a tailored pub/sub system in Java to route messages between microservices, coordinating trading actions.
- **Redis for Intra-Microservice Communication**:
  - **Redis Pub/Sub**: Handles low-priority messages, such as mouse pointer position updates for collaborative trading features.
  - **Redis Streams**: Ensures reliable delivery for high-priority messages, including user connections/disconnections, chat messages, and market orders.
- **Event-Driven Design**: Reacts to trading events (e.g., market orders, user updates) asynchronously, ensuring decoupled and responsive communication.
- **Distributed, Scalable Architecture**: We can scale horizontally the number of WebSocket Microservice instances. When a new backend instance spins up, all new WebSocket connections are routed to that instance until the number of connections on it is roughly the same as on all other instances. ie Least Connection Algorithm.

## 📚 Skills Demonstrated
This project highlights my expertise in:
- **Spring Microservices & Cloud**: Building a microservicecustom pub/sub system for inter-microservice communication.
- **
- **Distributed Systems**: Designing a multi-microservice architecture with Redis Pub/Sub and Streams for intra-microservice messaging.
- **Real-Time Communication**: Enabling WebSocket messaging with load balancing for trading actions.
- **Concurrency**: Managing threads, atomic operations, and bounded queues for performance.
- **Full-Stack Development**: Integrating a React frontend with a Java backend for a paper trading platform.

## 🌟 Why This Project?
The **RealTime WebSocket Microservice** reflects my passion for building scalable, real-time systems. By creating a custom pub/sub design for inter-microservice communication and leveraging Redis for intra-microservice messaging, this project powers a paper trading platform with real-time trading and collaboration features, showcasing my ability to solve complex challenges in distributed systems and full-stack development.

---

⭐ **Star this repo if you find it useful!** ⭐
