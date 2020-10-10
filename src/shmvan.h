#ifndef _SHMVAN_H_
#define _SHMVAN_H_

#include <pthread.h>
#include <unordered_map>
#include <sys/types.h>
#include <mutex>
#include <condition_variable>
#include <queue>
#include "ps/internal/van.h"

namespace ps {

#define RINGBUFFER_SIZE		(16*1024*1024)			//64MB
#define TEST_TIME

struct RingBuffer {
	unsigned int size;
	unsigned int in;
	unsigned int out;
	pthread_spinlock_t lock;
	unsigned char buf[RINGBUFFER_SIZE];
};

struct VanBuf {
	size_t	meta_size;
	size_t	data_num;					//data_num <= 64
	size_t	data_size[64];
	int sender_id;
	pthread_mutex_t connect_mutex;
	pthread_cond_t connect_cond;
	bool connected_flag;
#ifdef TEST_TIME
	double sender_start;
#endif
	struct RingBuffer rb;
};

template <typename T>
class Queue {
public:
	void Push(T val);
	T WaitAndPop();

private:
	std::queue<T> q;
	std::mutex mtx;
	std::condition_variable cv;
};

struct PIDBuf {
	int bind_flag;
	pid_t pid;
};

class SHMVAN : public Van{
public:
	SHMVAN() {}
	virtual ~SHMVAN() {}
	virtual void Start(int customer_id);
	virtual void Stop();
	virtual int Bind(const Node& node, int max_retry);
	virtual void Connect(const Node& node);
	virtual int RecvMsg(Message *msg);
	virtual int SendMsg(const Message &msg);

private:
	static void* SignalThread(void *args);
	static void SignalHandle(int signo, siginfo_t *info);
	void SetCurVan();
	void Notify(int signo, int pid);
	void WaitConnected(struct VanBuf *buf);
	void SignalRecv(siginfo_t *info);
	void SignalConnected(siginfo_t *info);
	void SignalConnect(siginfo_t *info);
	ssize_t Recv(struct RingBuffer *ring_buffer, void *buf, size_t len);
	ssize_t Send(struct RingBuffer *ring_buffer, const void *buf, size_t len);

	//test time
	double cpu_second(void);

private:
	pid_t pid;															//my pid

	//share my pid when bind
	int pid_shmid;														
	PIDBuf *pid_shm;																											
	static SHMVAN *cur_van;
	sigset_t mask;
	pthread_t signal_tid;
	
	std::unordered_map<pid_t, std::pair<int, VanBuf *> > sender;		//record sender's <pid, <shmid, VanBuf *> >
	std::unordered_map<pid_t, std::pair<int, VanBuf *> > recver;		//record recver's <pid, <shmid, VanBuf *> >
	std::unordered_map<int, pid_t>						 recver_pid;	//record recver's <node_id, pid>
	
	Queue<pid_t>										 pid_queue;
};

}
#endif


