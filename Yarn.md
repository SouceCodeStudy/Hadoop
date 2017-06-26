ResourceManager    
	功能  
			处理客户端请求  
			启动/监控Application Master  
			监控NodeManager  
			资源分配与调度（资源指的是CPU和IO和内存）  
		
NodeManager  
	功能  
		    单个节点上的资源管理和任务管理  
		    处理为自ResourceManager的命令   
		    处理来自Application Master的命令  
	
Application Master    
	功能  
		    数据切分    
		    为应用程序申请资源，并进一步分配给内部任务  
		    任务监控与容错  
		
Container  
	功能  
		    对任务运行环境的抽象  
		    描述一系列信息  
		    任务运行资源（节点、内存、CPU）  
		    任务启动命令  
		    任务运行环境  
	