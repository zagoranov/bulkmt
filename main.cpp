#include <iostream>
#include <fstream>
#include <string>
#include <queue>
#include <time.h>

#include <map>
#include <memory>

#include <thread>
#include <mutex>



typedef void(*write_function)(std::string&, std::ostream&);

class ThreadTask {
	bool bStopFlag;
	std::thread::id this_id;
	write_function func;
	std::queue<std::string> que;
	std::ostream& stream;
public:
	ThreadTask(write_function _func, std::ostream& _stream) : func(_func), stream(_stream), bStopFlag(true) { };
	void Start() {
		std::thread::id this_id = std::this_thread::get_id();
		std::cout << " --- Started " << this_id << std::endl;
		while (true) {
			if (!que.empty()) {
				func(que.front(), stream);
				que.pop();
			}
			if (bStopFlag && que.empty())
				break;
			std::this_thread::sleep_for(std::chrono::seconds(1));
		}
		std::cout << " --- Stopped " << this_id << std::endl;
	}
	void AddTask(std::string& s) {	que.emplace(s);	};
	void Stop() {
		bStopFlag = false;
	}
};


using Threads = std::map<std::shared_ptr<ThreadTask>, size_t>;
std::queue<std::shared_ptr<std::thread>> RealThreads;

class ThreadPool {
protected:
	Threads m_threads;
public:
	void AddThreads(size_t n, write_function func, std::ostream& stream) {
		for (size_t i = 0; i < n; ++i) {
			auto task = std::make_shared<ThreadTask>(func, stream);
			RealThreads.push(std::make_shared<std::thread>(&ThreadTask::Start, task));
			m_threads.emplace(task, 0);
		}
	}
	void ThreadExecute(std::string &s) {
		if (!m_threads.empty()) {
			auto mostFreeThreadIt = std::min(m_threads.begin(), m_threads.end(), [](auto &thr1, auto &thr2) { return thr1->second > thr2->second; });
			if (mostFreeThreadIt != m_threads.end()) {
				auto mostFreeThread = *mostFreeThreadIt;
				mostFreeThread.first->AddTask(s);
				mostFreeThread.second = mostFreeThread.second + 1;
			}
		}
	}
	void StopThreads() { 
		for(auto thr : m_threads) {
			thr.first->Stop();
		}
	}
};



class Writer {
public:
	virtual void update(std::string &s) = 0;		// Для вызова потока
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
	size_t N, n_cnt, n_brackets;
	Bulk bulk;
	std::vector<Writer*> subs;
public:
	BulkMechanics(size_t _N, Bulk &_bulk) : N(_N), bulk(_bulk), n_cnt(0), n_brackets(0) { }
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
					if (n_cnt == N) {
						Flash();
					}
				}
			}
	}
	void Flash() {
		std::string str = bulk.Retrive();
		if (!str.empty()) {
			for (auto s : subs) {
				s->update(str);
			}
		}
		n_cnt = 0;
	}
	void Subscribe(Writer *writer) {
		subs.push_back(writer);
	}

};


static std::mutex g_i_mutex;
static void StreamWrite(std::string &s, std::ostream& stream) {
	std::lock_guard<std::mutex> lock(g_i_mutex);
	stream << s << std::endl;
}


class screen_writer : public Writer, public ThreadPool {
public:
	screen_writer(BulkMechanics& mech, size_t iThreadPoolSize) {
		mech.Subscribe(this);
		AddThreads(iThreadPoolSize, &StreamWrite, std::cout);
	}
	void update(std::string &s) override {
		//std::cout << s << std::endl;
		ThreadExecute(s);
	}
};

class file_writer : public Writer, public ThreadPool {
	std::ofstream logfile;
public:
	file_writer(BulkMechanics& mech, size_t iThreadPoolSize) {
		mech.Subscribe(this);
		logfile.open("bulk" + std::to_string(time(NULL)) + ".log");
		AddThreads(iThreadPoolSize, &StreamWrite, logfile);
	}
	~file_writer() {
		logfile.close();
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
	BulkMechanics mech{ N, bulk };
	
	// 1 поток вывода на экран
	screen_writer scr_wr(mech, 1);
	
	// 2 потока вывода в файл
	file_writer file_wr(mech, 2);

	std::string line;
	while (std::getline(std::cin, line))
	{
		if (!line.empty())
			mech.Parse(line);
	}

	// Sending stop to objects
	scr_wr.StopThreads();
	file_wr.StopThreads();

	// Waiting each real thread to stop
	while (!RealThreads.empty()) {
		auto thr = RealThreads.front();
		if(thr->joinable())
			thr->join();
		RealThreads.pop();
	}

}



