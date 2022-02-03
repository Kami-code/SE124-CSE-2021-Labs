#ifndef mr_protocol_h_
#define mr_protocol_h_

#include <string>
#include <vector>

#include "rpc.h"

using namespace std;

#define REDUCER_COUNT 4

enum mr_tasktype {
	NONE = 0, // this flag means no task needs to be performed at this point
	MAP,
	REDUCE
};

class mr_protocol {
public:
	typedef int status;
	enum xxstatus { OK, RPCERR, NOENT, IOERR };
	enum rpc_numbers {
		asktask = 0xa001,
		submittask,
	};

	struct AskTaskResponse {
		// Lab2: Your definition here.
        int result;
        string filename;
        int reduce_id;
        int map_id;
	};

	struct AskTaskRequest {
		// Lab2: Your definition here.
	};

	struct SubmitTaskResponse {
		// Lab2: Your definition here.
	};

	struct SubmitTaskRequest {
		// Lab2: Your definition here.
	};

};

inline unmarshall & operator>>(unmarshall &u, mr_protocol::AskTaskResponse &a)
{
    u >> a.result;
    u >> a.filename;
    u >> a.reduce_id;
    u >> a.map_id;
    return u;
}

inline marshall & operator<<(marshall &m, mr_protocol::AskTaskResponse a)
{
    m << a.result;
    m << a.filename;
    m << a.reduce_id;
    m << a.map_id;
    return m;
}

#endif

