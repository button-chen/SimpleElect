# SimpleElect
简单选举算法与实现一致性


单机测试：
SimpleElect 9001 127.0.0.1:5001 127.0.0.1:5001 127.0.0.1:5002 127.0.0.1:5003
SimpleElect 9002 127.0.0.1:5002 127.0.0.1:5001 127.0.0.1:5002 127.0.0.1:5003
SimpleElect 9003 127.0.0.1:5003 127.0.0.1:5001 127.0.0.1:5002 127.0.0.1:5003


curl或者浏览器：
http://127.0.0.1:9001/set?key=name&val=button&expired=20000

输出： ok

curl或者浏览器:
http://127.0.0.1:9003/get?key=name

输出: button




