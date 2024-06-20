/*
 * BUILD COMMAND:
 * g++ rc_send_sched.cpp -o rc_send_sched -libverbs -lpthread
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netdb.h>
#include <malloc.h>
#include <getopt.h>
#include <arpa/inet.h>
#include <time.h>
#include <inttypes.h>
#include <infiniband/verbs.h>
#include <thread>
#include <queue>
#include <utility>
#include <mutex>
#include <atomic>

#define LATENCY_SIZE    4
#define BANDWITH_SIZE   524288

#define LATENCY_PORT	18515
#define BANDWIDTH_PORT	18516

enum {
	RECV_WRID = 1,
	SEND_WRID = 2,
};

enum app_type {
	NO_TYPE = 0,
    LATENCY = 1,
    BANDWIDTH = 2,
};

struct context {
	enum app_type app_type;
	struct ibv_context	*context;
	struct ibv_comp_channel *channel;
	struct ibv_pd		*pd;
	struct ibv_mr		*mr;
	union {
		struct ibv_cq		*cq;
		struct ibv_cq_ex	*cq_ex;
	} cq_s;
	struct ibv_qp		*qp;
	struct ibv_qp_ex	*qpx;
	char			*buf;
	int			 size;
	int			 send_flags;
	int			 rx_depth;
	struct ibv_port_attr     portinfo;
	uint64_t		 completion_timestamp_mask;
};

struct dest {
	int lid;
	int qpn;
	int psn;
	union ibv_gid gid;
};

static int page_size;
struct ibv_device	**dev_list;
struct ibv_device	*ib_dev;
char				*ib_devname = NULL;
char				*servername = NULL;
int					ib_port = 1;
int					size = 0;
enum ibv_mtu		mtu = IBV_MTU_1024;
unsigned int		rx_depth = 512;
unsigned int		iters = 1000;
int					sl = 0;
int					gidx = -1;
char				gid[33];
unsigned int		max_size = BANDWITH_SIZE;
enum app_type		priority = NO_TYPE;
std::queue<std::pair<struct context*, int>> latency_que;
std::mutex latque_mtx;
std::queue<std::pair<struct context*, int>> bandwidth_que;
std::mutex bwque_mtx;
std::atomic<bool> stopFlag(false);
double lat[25][2];
double bw[25][2];
std::mutex print_mtx;

void wire_gid_to_gid(const char *wgid, union ibv_gid *gid)
{
	char tmp[9];
	unsigned int v32;
	int i;
	uint32_t tmp_gid[4];

	for (tmp[8] = 0, i = 0; i < 4; ++i) {
		memcpy(tmp, wgid + i * 8, 8);
		sscanf(tmp, "%x", &v32);
		tmp_gid[i] = be32toh(v32);
	}
	memcpy(gid, tmp_gid, sizeof(*gid));
}

void gid_to_wire_gid(const union ibv_gid *gid, char wgid[])
{
	uint32_t tmp_gid[4];
	int i;

	memcpy(tmp_gid, gid, sizeof(tmp_gid));
	for (i = 0; i < 4; ++i)
		sprintf(&wgid[i * 8], "%08x", htobe32(tmp_gid[i]));
}

enum ibv_mtu mtu_to_enum(int mtu)
{
	switch (mtu) {
	case 256:  return IBV_MTU_256;
	case 512:  return IBV_MTU_512;
	case 1024: return IBV_MTU_1024;
	case 2048: return IBV_MTU_2048;
	case 4096: return IBV_MTU_4096;
	default:   return IBV_MTU_1024;
	}
}

enum app_type priority_to_enum(int pri)
{
	switch (pri) {
	case 1:
		printf("priority: LATENCY\n");
		return LATENCY;
	case 2:
		printf("priority: BANDWIDTH\n");
		return BANDWIDTH;
	default:
		printf("priority: NO_TYPE\n");
		return NO_TYPE;
	}
}

static int connect_ctx(struct context *ctx, int port, int my_psn,
			  enum ibv_mtu mtu, int sl,
			  struct dest *dest, int sgid_idx)
{
	struct ibv_qp_attr attr;
	memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = mtu;
    attr.dest_qp_num = dest->qpn;
    attr.rq_psn = dest->psn;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 12;
    attr.ah_attr.is_global = 0;
    attr.ah_attr.dlid = dest->lid;
    attr.ah_attr.sl = sl;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = port;

	if (dest->gid.global.interface_id) {
		attr.ah_attr.is_global = 1;
		attr.ah_attr.grh.hop_limit = 1;
		attr.ah_attr.grh.dgid = dest->gid;
		attr.ah_attr.grh.sgid_index = sgid_idx;
	}
	if (ibv_modify_qp(ctx->qp, &attr,
			  IBV_QP_STATE              |
			  IBV_QP_AV                 |
			  IBV_QP_PATH_MTU           |
			  IBV_QP_DEST_QPN           |
			  IBV_QP_RQ_PSN             |
			  IBV_QP_MAX_DEST_RD_ATOMIC |
			  IBV_QP_MIN_RNR_TIMER)) {
		fprintf(stderr, "Failed to modify QP to RTR\n");
		return 1;
	}

	attr.qp_state	    = IBV_QPS_RTS;
	attr.timeout	    = 14;
	attr.retry_cnt	    = 7;
	attr.rnr_retry	    = 7;
	attr.sq_psn	    = my_psn;
	attr.max_rd_atomic  = 1;
	if (ibv_modify_qp(ctx->qp, &attr,
			  IBV_QP_STATE              |
			  IBV_QP_TIMEOUT            |
			  IBV_QP_RETRY_CNT          |
			  IBV_QP_RNR_RETRY          |
			  IBV_QP_SQ_PSN             |
			  IBV_QP_MAX_QP_RD_ATOMIC)) {
		fprintf(stderr, "Failed to modify QP to RTS\n");
		return 1;
	}

	return 0;
}

static struct dest *client_exch_dest(const char *servername, int port,
						 const struct dest *my_dest)
{
	struct addrinfo *res, *t;
	struct addrinfo hints;
	memset(&hints, 0, sizeof(hints));
	hints.ai_family   = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;

	char service[6];
	char msg[sizeof "0000:000000:000000:00000000000000000000000000000000"];
	int n;
	int sockfd = -1;
	struct dest *rem_dest = NULL;
	char gid[33];

	if (sprintf(service, "%d", port) < 0)
		return NULL;

	n = getaddrinfo(servername, service, &hints, &res);

	if (n < 0) {
		fprintf(stderr, "%s for %s:%d\n", gai_strerror(n), servername, port);
		return NULL;
	}

	for (t = res; t; t = t->ai_next) {
		sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
		if (sockfd >= 0) {
			if (!connect(sockfd, t->ai_addr, t->ai_addrlen))
				break;
			close(sockfd);
			sockfd = -1;
		}
	}

	freeaddrinfo(res);

	if (sockfd < 0) {
		fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
		return NULL;
	}

	gid_to_wire_gid(&my_dest->gid, gid);
	sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn,
							my_dest->psn, gid);
	if (write(sockfd, msg, sizeof msg) != sizeof msg) {
		fprintf(stderr, "Couldn't send local address\n");
		goto out;
	}

	if (read(sockfd, msg, sizeof msg) != sizeof msg ||
	    write(sockfd, "done", sizeof "done") != sizeof "done") {
		perror("client read/write");
		fprintf(stderr, "Couldn't read/write remote address\n");
		goto out;
	}

	rem_dest = (dest *)malloc(sizeof *rem_dest);
	if (!rem_dest)
		goto out;

	sscanf(msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn,
						&rem_dest->psn, gid);
	wire_gid_to_gid(gid, &rem_dest->gid);

out:
	close(sockfd);
	return rem_dest;
}

static struct dest *server_exch_dest(struct context *ctx,
						 int ib_port, enum ibv_mtu mtu,
						 int port, int sl,
						 const struct dest *my_dest,
						 int sgid_idx)
{
	struct addrinfo *res, *t;
	struct addrinfo hints = {
		.ai_flags    = AI_PASSIVE,
		.ai_family   = AF_UNSPEC,
		.ai_socktype = SOCK_STREAM
	};
	char service[6];
	char msg[sizeof "0000:000000:000000:00000000000000000000000000000000"];
	int n;
	int sockfd = -1, connfd;
	struct dest *rem_dest = NULL;
	char gid[33];

	if (sprintf(service, "%d", port) < 0)
		return NULL;

	n = getaddrinfo(NULL, service, &hints, &res);

	if (n < 0) {
		fprintf(stderr, "%s for port %d\n", gai_strerror(n), port);
		return NULL;
	}

	for (t = res; t; t = t->ai_next) {
		sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
		if (sockfd >= 0) {
			n = 1;

			setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &n, sizeof n);

			if (!bind(sockfd, t->ai_addr, t->ai_addrlen))
				break;
			close(sockfd);
			sockfd = -1;
		}
	}

	freeaddrinfo(res);

	if (sockfd < 0) {
		fprintf(stderr, "Couldn't listen to port %d\n", port);
		return NULL;
	}

	listen(sockfd, 1);
	connfd = accept(sockfd, NULL, NULL);
	close(sockfd);
	if (connfd < 0) {
		fprintf(stderr, "accept() failed\n");
		return NULL;
	}

	n = read(connfd, msg, sizeof msg);
	if (n != sizeof msg) {
		perror("server read");
		fprintf(stderr, "%d/%d: Couldn't read remote address\n", n, (int) sizeof msg);
		goto out;
	}

	rem_dest = (dest *)malloc(sizeof *rem_dest);
	if (!rem_dest)
		goto out;

	sscanf(msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn,
							&rem_dest->psn, gid);
	wire_gid_to_gid(gid, &rem_dest->gid);

	if (connect_ctx(ctx, ib_port, my_dest->psn, mtu, sl, rem_dest,
								sgid_idx)) {
		fprintf(stderr, "Couldn't connect to remote QP\n");
		free(rem_dest);
		rem_dest = NULL;
		goto out;
	}


	gid_to_wire_gid(&my_dest->gid, gid);
	sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn,
							my_dest->psn, gid);
	if (write(connfd, msg, sizeof msg) != sizeof msg ||
	    read(connfd, msg, sizeof msg) != sizeof "done") {
		fprintf(stderr, "Couldn't send/recv local address\n");
		free(rem_dest);
		rem_dest = NULL;
		goto out;
	}


out:
	close(connfd);
	return rem_dest;
}

static struct context *init_ctx(struct ibv_device *ib_dev, int size,
					    int rx_depth, int port)
{
	struct context *ctx;
	int access_flags = IBV_ACCESS_LOCAL_WRITE;

	ctx = (context *)calloc(1, sizeof *ctx);
	if (!ctx)
		return NULL;

	ctx->size       = size;
	ctx->send_flags = IBV_SEND_SIGNALED;
	ctx->rx_depth   = rx_depth;

	ctx->buf = (char *)memalign(page_size, size);
	if (!ctx->buf) {
		fprintf(stderr, "Couldn't allocate work buf.\n");
		goto clean_ctx;
	}

	/* FIXME memset(ctx->buf, 0, size); */
	memset(ctx->buf, 0x7b, size);

	ctx->context = ibv_open_device(ib_dev);
	if (!ctx->context) {
		fprintf(stderr, "Couldn't get context for %s\n",
			ibv_get_device_name(ib_dev));
		goto clean_buffer;
	}

	ctx->channel = NULL;

	ctx->pd = ibv_alloc_pd(ctx->context);
	if (!ctx->pd) {
		fprintf(stderr, "Couldn't allocate PD\n");
		goto clean_comp_channel;
	}

	ctx->mr = ibv_reg_mr(ctx->pd, ctx->buf, size, access_flags);

	if (!ctx->mr) {
		fprintf(stderr, "Couldn't register MR\n");
		goto clean_pd;
	}

	ctx->cq_s.cq = ibv_create_cq(ctx->context, rx_depth + 1, NULL,
					ctx->channel, 0);

	if (!ctx->cq_s.cq) {
		fprintf(stderr, "Couldn't create CQ\n");
		goto clean_mr;
	}

	{
		struct ibv_qp_attr attr;
		memset(&attr, 0, sizeof(attr));
		struct ibv_qp_init_attr init_attr;
		memset(&init_attr, 0, sizeof(init_attr));
		init_attr.send_cq = ctx->cq_s.cq;
		init_attr.recv_cq = ctx->cq_s.cq;
		init_attr.cap.max_send_wr  = rx_depth;
		init_attr.cap.max_recv_wr  = rx_depth;
		init_attr.cap.max_send_sge = 1;
		init_attr.cap.max_recv_sge = 1;
		init_attr.qp_type = IBV_QPT_RC;

		ctx->qp = ibv_create_qp(ctx->pd, &init_attr);

		if (!ctx->qp)  {
			fprintf(stderr, "Couldn't create QP\n");
			goto clean_cq;
		}

		ibv_query_qp(ctx->qp, &attr, IBV_QP_CAP, &init_attr);
		// if (init_attr.cap.max_inline_data >= size)
		// 	ctx->send_flags |= IBV_SEND_INLINE;
	}

	{
		struct ibv_qp_attr attr;
		memset(&attr, 0, sizeof(attr));
		attr.qp_state        = IBV_QPS_INIT;
		attr.pkey_index      = 0;
		attr.port_num        = port;
		attr.qp_access_flags = 0;

		if (ibv_modify_qp(ctx->qp, &attr,
				  IBV_QP_STATE              |
				  IBV_QP_PKEY_INDEX         |
				  IBV_QP_PORT               |
				  IBV_QP_ACCESS_FLAGS)) {
			fprintf(stderr, "Failed to modify QP to INIT\n");
			goto clean_qp;
		}
	}

	return ctx;

clean_qp:
	ibv_destroy_qp(ctx->qp);

clean_cq:
	ibv_destroy_cq(ctx->cq_s.cq);

clean_mr:
	ibv_dereg_mr(ctx->mr);

clean_pd:
	ibv_dealloc_pd(ctx->pd);

clean_comp_channel:
	if (ctx->channel)
		ibv_destroy_comp_channel(ctx->channel);

clean_device:
	ibv_close_device(ctx->context);

clean_buffer:
	free(ctx->buf);

clean_ctx:
	free(ctx);

	return NULL;
}

static int close_ctx(struct context *ctx)
{
	if (ibv_destroy_qp(ctx->qp)) {
		fprintf(stderr, "Couldn't destroy QP\n");
		return 1;
	}

	if (ibv_destroy_cq(ctx->cq_s.cq)) {
		fprintf(stderr, "Couldn't destroy CQ\n");
		return 1;
	}

	if (ibv_dereg_mr(ctx->mr)) {
		fprintf(stderr, "Couldn't deregister MR\n");
		return 1;
	}

	if (ibv_dealloc_pd(ctx->pd)) {
		fprintf(stderr, "Couldn't deallocate PD\n");
		return 1;
	}

	if (ctx->channel) {
		if (ibv_destroy_comp_channel(ctx->channel)) {
			fprintf(stderr, "Couldn't destroy completion channel\n");
			return 1;
		}
	}

	if (ibv_close_device(ctx->context)) {
		fprintf(stderr, "Couldn't release context\n");
		return 1;
	}

	free(ctx->buf);
	free(ctx);

	return 0;
}

static int post_recv(struct context *ctx, int n)
{
	struct ibv_sge list;
	memset(&list, 0, sizeof(list));
	list.addr	= (uintptr_t) ctx->buf;
	list.length = ctx->size;
	list.lkey	= ctx->mr->lkey;

	struct ibv_recv_wr wr;
	memset(&wr, 0, sizeof(wr));
	wr.wr_id	    = RECV_WRID;
	wr.sg_list    = &list;
	wr.num_sge    = 1;

	struct ibv_recv_wr *bad_wr;
	int i;

	for (i = 0; i < n; ++i)
		if (ibv_post_recv(ctx->qp, &wr, &bad_wr))
			break;

	return i;
}

static int post_send(struct context *ctx, int size)
{
	struct ibv_sge list;
	memset(&list, 0, sizeof(list));
	list.addr	= (uintptr_t) ctx->buf;
	list.length = size;
	list.lkey	= ctx->mr->lkey;

	struct ibv_send_wr wr;
	memset(&wr, 0, sizeof(wr));
	wr.wr_id	    = SEND_WRID;
	wr.sg_list    = &list;
	wr.num_sge    = 1;
	wr.opcode     = IBV_WR_SEND;
	wr.send_flags = ctx->send_flags;

	struct ibv_send_wr *bad_wr;

	// if (ctx->app_type == LATENCY) {
	// 	printf("a ");
	// } else if (ctx->app_type == BANDWIDTH) {
	// 	printf("b ");
	// }

	return ibv_post_send(ctx->qp, &wr, &bad_wr);
}

int poll_cq(struct context * ctx, int nums){
	struct ibv_wc wc[nums];
	uint32_t nums_cqe = 0;
	while(nums_cqe < nums){
		int cqes = ibv_poll_cq(ctx->cq_s.cq, nums, wc);
		if(cqes < 0 ){
			 fprintf(stderr, "poll cq error. nums_cqe %d\n",nums_cqe);
			 return -1 ;
		}
		nums_cqe += cqes;
		for(int j  = 0;j < cqes; ++j){
			if (wc[j].status != IBV_WC_SUCCESS) {
			    fprintf(stderr, "Failed status %s (%d) for wr_id %d\n",
			            ibv_wc_status_str(wc[j].status), wc[j].status, (int)wc[j].wr_id);
			    return -1;
			}
		}
	}
	return nums_cqe;
}

static void usage(const char *argv0)
{
	printf("Usage:\n");
	printf("  %s            start a server and wait for connection\n", argv0);
	printf("  %s <host>     connect to server at <host>\n", argv0);
	printf("\n");
	printf("Options:\n");
	printf("  -d, --ib-dev=<dev>     use IB device <dev> (default first device found)\n");
	printf("  -i, --ib-port=<port>   use port <port> of IB device (default 1)\n");
	printf("  -s, --size=<size>      size of message to exchange (default 4096)\n");
	printf("  -m, --mtu=<size>       path MTU (default 1024)\n");
	printf("  -r, --rx-depth=<dep>   number of receives to post at a time (default 500)\n");
	printf("  -n, --iters=<iters>    number of exchanges (default 1000)\n");
	printf("  -l, --sl=<sl>          service level value\n");
	printf("  -g, --gid-idx=<gid index> local port gid index\n");
	printf("  -p, --priority		 Prioritize running applications of specified types\n");
}

int post_send_poll(struct context *ctx, unsigned int iters, unsigned int size)
{
	int i, nums_cqe = 0;
	int wc_count = 50;

	for (i = 1; i <= iters; i++) {
		if (priority == NO_TYPE) {
			if (post_send(ctx, size)) {
				fprintf(stderr, "Couldn't post send\n");
				printf("nums_cqe %d iter %d \n",nums_cqe,i);
				return -1;
			}
		} else if (priority == LATENCY) {
			if (ctx->app_type == LATENCY) {
				if (post_send(ctx, size)) {
					fprintf(stderr, "Couldn't post send\n");
					printf("nums_cqe %d iter %d \n",nums_cqe,i);
					return -1;
				}
			} else {
				std::unique_lock<std::mutex> lock(bwque_mtx);
				bandwidth_que.push(std::make_pair(ctx, size));
				lock.unlock();
			}
		} else {
			printf("priority type error\n");
		}

		if (i % wc_count == 0) {
			int ret = poll_cq(ctx, wc_count);
			if(ret < 0){
				perror("error in poll");
				return -1;
			}
			nums_cqe += ret;
		}
	}
	// 还剩下 iters % wc_count 个 cqe 没有 poll
	if (iters % wc_count != 0) {
		int ret = poll_cq(ctx, iters % wc_count);
		if(ret < 0){
			perror("error in poll");
			return -1;
		}
		nums_cqe += ret;
	}
	return 0;
}

int post_recv_poll(struct context *ctx, int *routs, unsigned int iters)
{
	int rcnt = 0, wc_count = 50, ne;
	struct ibv_wc wc[wc_count];

	while (rcnt < iters) {
		// 如果剩下的 iters-rcnt 小于 wc_count, 则动态减小 wc_count
		if (iters - rcnt < wc_count)
			wc_count = iters - rcnt;

		if (*routs < wc_count) {
			*routs += post_recv(ctx, ctx->rx_depth - *routs);
			if (*routs < ctx->rx_depth) {
				fprintf(stderr, "Couldn't post receive (%d)\n", *routs);
				return -1;
			}
		}

		do {
			ne = ibv_poll_cq(ctx->cq_s.cq, wc_count, wc);
			if (ne < 0) {
				fprintf(stderr, "poll CQ failed %d\n", ne);
				return 1;
			}
		} while (ne < 1);
		rcnt += ne;
		*routs -= ne;
	}
	return 0;
}

int test_time(struct context *ctx, int iters, int *routs)
{
	struct timeval start, end;
	int rx_depth = ctx->rx_depth;
	unsigned int size = LATENCY_SIZE;
	int i = 0;
	
	while (size <= max_size) {
		//start send
		if (gettimeofday(&start, NULL)) {
			perror("gettimeofday");
			return -1;
		}
		
		if (servername) {
			if (post_send_poll(ctx, iters, ctx->app_type==LATENCY?LATENCY_SIZE:size) < 0) {
				fprintf(stderr, "Couldn't post_send_poll1\n");
				return -1;
			}
		} else {
			if (post_recv_poll(ctx, routs, iters) < 0) {
				fprintf(stderr, "Couldn't post_recv_poll\n");
				return -1;
			}
		}

		//end send
		if (gettimeofday(&end, NULL)) {
			perror("gettimeofday");
			return -1;
		}

		// record test time
		std::unique_lock<std::mutex> lock(print_mtx);
		if (ctx->app_type == LATENCY) {
			lat[i][0] = (end.tv_sec - start.tv_sec) * 1000000 +
				(end.tv_usec - start.tv_usec);
			long long bytes = (long long) LATENCY_SIZE * iters;
			bw[i][0] = bytes*8.0/(lat[i][0])/1000;
		} else {
			lat[i][1] = (end.tv_sec - start.tv_sec) * 1000000 +
				(end.tv_usec - start.tv_usec);
			long long bytes = (long long) size * iters;
			bw[i][1] = bytes*8.0/(lat[i][1])/1000;
		}
		lock.unlock();

		if(size < max_size && size*2 > max_size){
			size = max_size;
		}else{
			size *= 2;
		}
		i++;
	}
	return 0;
}

void print_time() {
	int size = LATENCY_SIZE, i = 0;

	printf("---------------------------------------------------------------------------------------\n");
	printf("                    RDMA Send Sched Benchmark\n");
	printf("%-20s : %s\n", "Connection type", "RC");
	printf("%-20s : %d\n", "Number of qps", 2);
	printf("%-20s : %d\n", "RX depth", rx_depth);
	printf("%-20s : %s\n", "Device", ibv_get_device_name(ib_dev));
	printf("%-20s : %d\n", "Priority", priority);
	printf("---------------------------------------------------------------------------------------\n");
	printf("%-20s %-20s %-20s %-20s %-20s %-20s %-20s\n", "#iterations", "#bytes[Lat App]", "BW[Gbps]", "Lat[us]",
		"#bytes[Bw App]", "Bandwidth[Gbps]", "Latency[us]");

	while (size <= max_size) {
		printf("%-20d %-20d %-20.3lf %-20.0f %-20d %-20.3lf %-20.0f\n", 
			iters, LATENCY_SIZE, bw[i][0], lat[i][0], size, bw[i][1], lat[i][1]);

		if(size < max_size && size*2 > max_size){
			size = max_size;
		}else{
			size *= 2;
		}
		i++;
	}
}

void send_recv_thread(enum app_type type)
{
    struct context	*ctx;
	struct dest		my_dest;
	struct dest		*rem_dest;
	int				routs = 0;
	int				rcnt, scnt;
	int port = type == LATENCY ? LATENCY_PORT : BANDWIDTH_PORT;

    ctx = init_ctx(ib_dev, max_size, rx_depth, ib_port);
	if (!ctx) {
        fprintf(stderr, "Couldn't create ctx\n");
        return;
    }
	ctx->app_type = type;

	if (!servername) {
		routs = post_recv(ctx, ctx->rx_depth);
		if (routs < ctx->rx_depth) {
			fprintf(stderr, "Couldn't post receive (%d)\n", routs);
			return;
		}
	}

	if (ibv_query_port(ctx->context, ib_port, &ctx->portinfo)) {
		fprintf(stderr, "Couldn't get port info\n");
		return;
	}

	my_dest.lid = ctx->portinfo.lid;
	if (ctx->portinfo.link_layer != IBV_LINK_LAYER_ETHERNET &&
							!my_dest.lid) {
		fprintf(stderr, "Couldn't get local LID\n");
		return;
	}

	if (gidx >= 0) {
		if (ibv_query_gid(ctx->context, ib_port, gidx, &my_dest.gid)) {
			fprintf(stderr, "can't read sgid of index %d\n", gidx);
			return;
		}
	} else
		memset(&my_dest.gid, 0, sizeof my_dest.gid);

	my_dest.qpn = ctx->qp->qp_num;
	my_dest.psn = lrand48() & 0xffffff;
	inet_ntop(AF_INET6, &my_dest.gid, gid, sizeof gid);

	if (servername)
		rem_dest = client_exch_dest(servername, port, &my_dest);
	else
		rem_dest = server_exch_dest(ctx, ib_port, mtu, port, sl,
								&my_dest, gidx);

	if (!rem_dest) {
		fprintf(stderr, "Couldn't exchange connection data\n");
		return;
	}

	inet_ntop(AF_INET6, &rem_dest->gid, gid, sizeof gid);

	if (servername)
		if (connect_ctx(ctx, ib_port, my_dest.psn, mtu, sl, rem_dest,
					gidx)) {
			fprintf(stderr, "Couldn't connect to remote QP\n");
			return;
		}

	if (test_time(ctx, iters, &routs) < 0) {
		fprintf(stderr, "test_time error\n");
		return;
	}

	if (close_ctx(ctx))
		return;
	free(rem_dest);
}

void scheduler_thread()
{
	printf("scheduler_thread come in\n");
	// if (priority == LATENCY) {
	int i = 0;
	while (!stopFlag) {
		if (!bandwidth_que.empty()) {
			std::unique_lock<std::mutex> lock(bwque_mtx);
			std::pair<struct context*, int> element = bandwidth_que.front();
			bandwidth_que.pop();
			lock.unlock();

			if (post_send(element.first, element.second)) {
				fprintf(stderr, "Couldn't post_send_send bandwidth\n");
				return;
			}
		}
	}
	// } else if (priority == BANDWIDTH) {
	// 	while (i < n) {
	// 		if (!latency_que.empty()) {
	// 			struct context *ctx = latency_que.front();
	// 			latency_que.pop();
	// 			if (post_send(ctx)) {
	// 				fprintf(stderr, "Couldn't post send latency\n");
	// 			}
	// 			i++;
	// 		}
	// 	}
	// } else {
	// 	fprintf(stderr, "priority type error\n");
	// }
	return;
}

int main(int argc, char *argv[])
{
	srand48(getpid() * time(NULL));

	while (1) {
		int c;

		static struct option long_options[] = {
			// { "port",     1, NULL, 'p' },
			{ "ib-dev",   1, NULL, 'd' },
			{ "ib-port",  1, NULL, 'i' },
			{ "size",     1, NULL, 's' },
			{ "mtu",      1, NULL, 'm' },
			{ "rx-depth", 1, NULL, 'r' },
			{ "iters",    1, NULL, 'n' },
			{ "sl",       1, NULL, 'l' },
			{ "gid-idx",  1, NULL, 'g' },
			{ "priority", 1, NULL, 'p'},
			{ NULL,		  0, NULL, 0 }  // 结尾元素，必要以表示数组结束
		};

		c = getopt_long(argc, argv, "d:i:s:m:r:n:l:g:p:",
				long_options, NULL);

		if (c == -1)
			break;

		switch (c) {
		case 'd':
			ib_devname = strdup(optarg);
			break;

		case 'i':
			ib_port = strtol(optarg, NULL, 0);
			if (ib_port < 1) {
				usage(argv[0]);
				return 1;
			}
			break;

		case 's':
			max_size = strtoul(optarg, NULL, 0);
			break;

		case 'm':
			mtu = mtu_to_enum(strtol(optarg, NULL, 0));
			if (mtu == 0) {
				usage(argv[0]);
				return 1;
			}
			break;

		case 'r':
			rx_depth = strtoul(optarg, NULL, 0);
			break;

		case 'n':
			iters = strtoul(optarg, NULL, 0);
			break;

		case 'l':
			sl = strtol(optarg, NULL, 0);
			break;

		case 'g':
			gidx = strtol(optarg, NULL, 0);
			break;
		
		case 'p':
			priority = priority_to_enum(strtol(optarg, NULL, 0));
			break;

		default:
			usage(argv[0]);
			return 1;
		}
	}

	if (optind == argc - 1)
		servername = strdup(argv[optind]);
	else if (optind < argc) {
		usage(argv[0]);
		return 1;
	}

	page_size = sysconf(_SC_PAGESIZE);

	dev_list = ibv_get_device_list(NULL);
	if (!dev_list) {
		perror("Failed to get IB devices list");
		return 1;
	}

	if (!ib_devname) {
		ib_dev = *dev_list;
		if (!ib_dev) {
			fprintf(stderr, "No IB devices found\n");
			return 1;
		}
	} else {
		int i;
		for (i = 0; dev_list[i]; ++i)
			if (!strcmp(ibv_get_device_name(dev_list[i]), ib_devname))
				break;
		ib_dev = dev_list[i];
		if (!ib_dev) {
			fprintf(stderr, "IB device %s not found\n", ib_devname);
			return 1;
		}
	}

	if (servername) {
		std::thread sched_thread;
		if (priority != NO_TYPE) {
			sched_thread = std::thread(scheduler_thread);
		}
		std::thread bandwidth_thread(send_recv_thread, BANDWIDTH);
		std::thread latency_thread(send_recv_thread, LATENCY);
        
		bandwidth_thread.join();
		latency_thread.join();
		// 设置标志位以通知线程停止
		stopFlag = true;
		if (priority != NO_TYPE && sched_thread.joinable()) {
			sched_thread.join();
		}
	} else {
		printf("************************************\n");
		printf("* Waiting for client to connect... *\n");
		printf("************************************\n");
		std::thread recv_bandwidth_thread(send_recv_thread, BANDWIDTH);
		std::thread recv_latency_thread(send_recv_thread, LATENCY);

		recv_bandwidth_thread.join();
		recv_latency_thread.join();
	}
	print_time();

	ibv_free_device_list(dev_list);
	return 0;
}
