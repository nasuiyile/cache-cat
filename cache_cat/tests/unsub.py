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

def subscriber():
    """订阅者线程：支持动态取消订阅"""
    pubsub = r.pubsub()
    pubsub.subscribe('news', 'sports')

    print("订阅者启动，等待消息...")
    print("订阅的频道: news, sports\n")

    # 消息计数器
    news_count = 0

    for message in pubsub.listen():
        if message['type'] == 'message':
            channel = message['channel']
            data = message['data']
            print(f"订阅者收到 [{channel}]: {data}")

            # 收到3条 news 消息后取消订阅 news 频道
            if channel == 'news':
                news_count += 1
                if news_count >= 3:
                    print("\n>>> 取消订阅 news 频道 <<<\n")
                    pubsub.unsubscribe('news')

        # 检查是否还有订阅的频道
        elif message['type'] == 'unsubscribe':
            print(f"已取消订阅频道: {message['channel']}")
            # 如果所有频道都取消订阅了，退出循环
            # data 表示剩余订阅数量
            if message['data'] == 0:
                print("所有频道已取消订阅，退出监听")
                break

# 启动订阅者线程
sub_thread = threading.Thread(target=subscriber, daemon=True)
sub_thread.start()

time.sleep(0.5)

# 发布消息
for i in range(50):
    r.publish('news', f"新闻消息{i+1}")
    r.publish('sports', f"体育消息{i+1}")
    time.sleep(1)

sleep(10000000)
print("\n程序结束")