import redis
import threading
import time

def test_psubscribe_and_punsubscribe():
    """
    测试 Redis 的 PSUBSCRIBE 和 PUNSUBSCRIBE 命令。
    流程：
    1. 创建一个订阅客户端，订阅模式 'news.*'。
    2. 启动一个线程监听订阅的消息。
    3. 主线程稍等片刻，然后发布匹配该模式的消息（如 'news.sports'）。
    4. 验证订阅客户端收到消息。
    5. 退订模式 'news.*'。
    6. 再次发布消息，确认不再收到。
    """
    # 连接 Redis（确保 Redis 服务正在运行）
    redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
    pubsub = redis_client.pubsub()

    # 用于收集接收到的消息
    received_messages = []

    def message_listener():
        """在独立线程中持续监听消息"""
        for message in pubsub.listen():
            # listen() 会返回不同类型的消息（subscribe, psubscribe, message, unsubscribe, punsubscribe）
            if message['type'] == 'pmessage':  # 模式订阅的消息
                print(f"[收到消息] 模式: {message['pattern']}, 频道: {message['channel']}, 数据: {message['data']}")
                received_messages.append(message)
            elif message['type'] in ('psubscribe', 'punsubscribe'):
                print(f"[事件] {message['type']}: {message['channel']} (模式: {message.get('pattern', 'N/A')})")
            # 忽略其他类型（如 'subscribe', 'unsubscribe'）因为我们使用的是模式订阅

    # 1. 订阅模式
    print("=== 1. 开始订阅模式 'news.*' ===")
    pubsub.psubscribe('news.*')  # 注意：psubscribe 是立即生效的，但会触发一个 'psubscribe' 消息

    # 启动监听线程
    listener_thread = threading.Thread(target=message_listener, daemon=True)
    listener_thread.start()

    # 等待订阅生效
    time.sleep(0.5)

    # 2. 发布匹配的消息
    print("\n=== 2. 发布消息到 'news.sports' ===")
    redis_client.publish('news.sports', 'Football match started!')
    time.sleep(0.5)

    # 3. 发布不匹配的消息（不应收到）
    print("\n=== 3. 发布消息到 'tech.python'（不匹配模式）===")
    redis_client.publish('tech.python', 'New version 3.12 released')
    time.sleep(0.5)

    # 4. 发布另一条匹配的消息
    print("\n=== 4. 发布消息到 'news.weather' ===")
    redis_client.publish('news.weather', 'Sunny tomorrow')
    time.sleep(0.5)

    # 5. 退订模式
    print("\n=== 5. 退订模式 'news.*' ===")
    pubsub.punsubscribe('news.*')  # 退订模式，会触发 'punsubscribe' 消息
    time.sleep(0.5)

    # 6. 退订后再次发布匹配消息，不应再收到
    print("\n=== 6. 退订后再发布消息到 'news.sports' ===")
    redis_client.publish('news.sports', 'This should NOT be received')
    time.sleep(0.5)

    # 输出统计结果
    print("\n=== 结果统计 ===")
    print(f"总共收到模式消息数: {len(received_messages)}")
    for i, msg in enumerate(received_messages, 1):
        print(f"  {i}. 频道: {msg['channel']} -> {msg['data']}")

    # 清理资源
    pubsub.close()
    redis_client.close()

if __name__ == "__main__":
    # 注意：需要先安装 redis 库：pip install redis
    try:
        test_psubscribe_and_punsubscribe()
    except redis.exceptions.ConnectionError as e:
        print(f"连接 Redis 失败，请确保 Redis 服务已启动且可访问。\n错误信息: {e}")