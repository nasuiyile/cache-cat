from time import sleep
import redis
import time
import threading

r = redis.Redis(
    db=0,
    host='localhost',
    port=6379,
    decode_responses=True
)
print()


def subscriber():
    """订阅者线程：持续接收多个频道的消息"""
    pubsub = r.pubsub()
    # 订阅两个频道
    pubsub.subscribe('news', 'sports')

    print("订阅者启动，等待消息...")
    print("订阅的频道: news, sports\n")

    for message in pubsub.listen():
        if message['type'] == 'message':
            # 显示消息来自哪个频道
            channel = message['channel']
            data = message['data']
            print(f"订阅者收到 [{channel}]: {data}")


def publisher():
    """发布者线程：向两个频道交替发布消息"""
    for i in range(5):
        # 向 news 频道发送消息
        msg1 = f"新闻消息{i + 1}"
        r.publish('news', msg1)
        print(f"发布者发送 [news]: {msg1}\n")

        # 向 sports 频道发送消息
        msg2 = f"体育消息{i + 1}"
        r.publish('sports', msg2)
        print(f"发布者发送 [sports]: {msg2}\n")

        print("-" * 40)
        time.sleep(2)


# 启动订阅者线程（设置为守护线程，主程序退出时自动结束）
sub_thread = threading.Thread(target=subscriber, daemon=True)
sub_thread.start()

# 主线程等待一下确保订阅者已连接
time.sleep(0.5)

# 在主线程中运行发布者
publisher()

print("\n发布完成，程序退出")