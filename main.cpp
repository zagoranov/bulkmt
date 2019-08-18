#include <iostream>
#include <fstream>
#include <string>
#include <queue>
#include <time.h>

#include <map>
#include <memory>
#include <thread>
#include <mutex>
#include <limits>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <atomic>
#include <algorithm>

enum class streamType
{
	Screen, 
	File
};

typedef void(*write_function)(std::string&, std::ostream&);

static std::mutex g_i_mutex;
static void StreamWrite(std::string &s, std::ostream& stream) {
	std::lock_guard<std::mutex> lock(g_i_mutex);
	stream << s << std::endl;
}


class ThreadTask {
	std::atomic<bool> bStopFlag;
	std::thread::id this_id;
	std::queue<std::string> que;
	std::ostream* stream;
	streamType s_t;
	size_t command_cnt, blocks_cnt;
	
	std::mutex m;
	std::condition_variable cond_var;
public:
	ThreadTask(std::ostream& _stream) : stream(&_stream), bStopFlag(false), command_cnt(0), blocks_cnt(0) { };
	ThreadTask(streamType _s_t, size_t i) : bStopFlag(false), s_t(_s_t), command_cnt(0), blocks_cnt(0) {
		if (_s_t == streamType::File) {
			stream = new std::ofstream("bulk" + std::to_string(time(NULL)) + "_" + std::to_string(i) + ".log", std::ofstream::out);
		}
		else
			if (_s_t == streamType::Screen) {
				stream = &std::cout;
			}
	}
	~ThreadTask() { 
		if (s_t == streamType::File)
			dynamic_cast<std::ofstream*>(stream)->close();
	}
	void Start() {
		this_id = std::this_thread::get_id();
		std::cout << " --- Started " << this_id << std::endl;
		std::unique_lock<std::mutex> lock(m);

		while ( !(bStopFlag && que.empty()) ) {
			while (que.empty() && !bStopFlag) {
				cond_var.wait(lock);
			}
			if (!que.empty()) {
				StreamWrite(que.front(), *stream);
				que.pop();
			}
		}
		std::cout << " --- Stopped " << this_id << std::endl;
	}

	void AddTask(std::string& s) {	
		std::unique_lock<std::mutex> lock(m);
		command_cnt += std::count(s.begin(), s.end(), ',') + 1;
		++blocks_cnt;
		que.emplace(s);	
		cond_var.notify_one();
	};
	void Stop() {
		std::unique_lock<std::mutex> lock(m);
		bStopFlag = true;
		cond_var.notify_one();
	}
	void ShowStatistics() {
		std::unique_lock<std::mutex> lock(m);
		std::cout << "Thread id " << this_id << " blocks proceeded " << blocks_cnt << " commands proceeded " << command_cnt << std::endl;
	}
};


using Threads = std::map<std::shared_ptr<ThreadTask>, size_t>;
std::queue<std::shared_ptr<std::thread>> RealThreads;

class ThreadPool {
protected:
	Threads m_threads;
public:
	void AddThread(streamType s_t, size_t i) {
			auto task = std::make_shared<ThreadTask>(s_t, i);
			RealThreads.push(std::make_shared<std::thread>(&ThreadTask::Start, task));
			m_threads.emplace(task, 0);
	}
	void ThreadExecute(std::string &s) {
		if (!m_threads.empty()) {
			std::shared_ptr<std::pair<std::shared_ptr<ThreadTask>, unsigned int>> mostFreeThread;
			unsigned imin = std::numeric_limits<unsigned int>::max();
			for (auto &thr : m_threads) {
				if (thr.second < imin) {
					imin = thr.second;
					mostFreeThread = std::make_shared<std::pair<std::shared_ptr<ThreadTask>, unsigned int>>(thr);
				}
			}
			assert(mostFreeThread);
			if (!mostFreeThread) { return; }	// oopsie doopsie
			m_threads[(*mostFreeThread).first] = (*mostFreeThread).second + 1;
			(*mostFreeThread).first->AddTask(s);
		}
	}
	void StopThreads() { 
		for (auto tsk = m_threads.begin(); tsk != m_threads.end(); ++tsk) {
			tsk->first->Stop();
		}
	}
	void AllStatistics() const {
		for (auto tsk = m_threads.begin(); tsk != m_threads.end(); ++tsk)
			tsk->first->ShowStatistics();
	}
};



class Writer {
public:
	virtual void update(std::string &s) = 0;
};


class Bulk {
	std::queue<std::string> bulk;
public:
	void Add(std::string &str) {
		bulk.emplace(str);
	}
	std::string Retrive() {
		std::string res = "";
		if (!bulk.empty()) {
			res = "bulk: ";
			while (!bulk.empty()) {
				res += bulk.front();
				bulk.pop();
				if (!bulk.empty())
					res += ", ";
			}
		}
		return res;
	}
};


class BulkMechanics {
	size_t Nmax, n_cnt, n_brackets;
	Bulk bulk;
	std::vector<Writer*> subscriptors;
public:
	BulkMechanics(size_t _Nmax, Bulk &_bulk) : Nmax(_Nmax), bulk(_bulk), n_cnt(0), n_brackets(0) { }
	void Parse(std::string &str) {
		if (str == "{") {
			if(n_brackets == 0)
				Flash();
			++n_brackets;
		}
		else 
			if (str == "}") {
				if (n_brackets > 0) {
					if (n_brackets == 1) {
						Flash();
					}
					--n_brackets;
				}
			}
			else {
				bulk.Add(str);
				if (n_brackets == 0) {
					++n_cnt;
					if (n_cnt == Nmax) {
						Flash();
					}
				}
			}
	}
	void Flash() {
		std::string str = bulk.Retrive();
		if (!str.empty()) {
			for (auto& s : subscriptors) {
				s->update(str);
			}
		}
		n_cnt = 0;
	}
	void Subscribe(Writer *writer) {
		subscriptors.push_back(writer);
	}

};


class the_writer : public Writer, public ThreadPool {
public:
	the_writer(BulkMechanics& mech, streamType strType, size_t iThreadPoolSize) {
		mech.Subscribe(this);
		for (size_t i = 0; i < iThreadPoolSize; ++i) {
			AddThread(strType, i);
		}
	}
	void update(std::string &s) override {
		ThreadExecute(s);
	}
};

int main(int argc, char* argv[])
{
	if (argc < 2) {
		std::cout << "You must provide the N argument\n" << std::endl; 
		return 1;
	}
	size_t N = atoi(argv[1]);
	Bulk bulk;
	BulkMechanics bulkMechanics{ N, bulk };
	
	the_writer screen_writer(bulkMechanics, streamType::Screen, 1);
	the_writer file_writer(bulkMechanics, streamType::File, 2);

	std::string line;
	while (std::getline(std::cin, line) && line != "q")
	{
		if (!line.empty())
			bulkMechanics.Parse(line);
	}

	// Statistics
	screen_writer.AllStatistics();
	file_writer.AllStatistics();

	// Sending stop to objects
	screen_writer.StopThreads();
	file_writer.StopThreads();

	
	// Waiting each real thread to stop
	while (!RealThreads.empty()) {
		auto thr = RealThreads.front();
		try {
			if (thr->joinable())
				thr->join();
			//thr->detach();
		}
		catch (...) {};
		RealThreads.pop();
	}
}
