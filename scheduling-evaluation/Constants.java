package scheduling_evaluation;

public class Constants {

	public static final int USER_COUNT					= 1;

	/* Resource count */
	// Assumption: number of resources = number of physical machines (in 1 datacenter)
	public static final int SMARTPHONE_RESOURCE_COUNT	= 10;
	public static final int RASPBERRY_PI_RESOURCE_COUNT	= 5;
	public static final int CLOUD_RESOURCE_COUNT		= 5;

	/* Task count */
	public static final int TASK_COUNT					= 2000;			// Number of tasks/ cloudlets
	// F * TASK_COUNT read tasks and (1-F) * TASK_COUNT write tasks
	public static final double F						= 0.5;			// Heterogeneity factor: {0.0, 0.25, 0.5, 0.75, 1.0}
	// Read task distribution: F_RT1 + F_RT2 + F_RT3 + F_RT4 = 1
	public static final double F_RT1					= 0.25;
	public static final double F_RT2					= 0.25;
	public static final double F_RT3					= 0.25;
	public static final double F_RT4					= 0.25;
	// Write task distribution: F_WT1 + F_WT2 + F_WT3 + F_WT4 = 1
	public static final double F_WT1					= 0.25;
	public static final double F_WT2					= 0.25;
	public static final double F_WT3					= 0.25;
	public static final double F_WT4					= 0.25;

	/* Cloud physical machine parameters */
	public static final int CLOUD_PM_PES_NUMBER							= 1;		// Number of CPUs
	public static final double CLOUD_PM_MIPS							= 1000000;	// MIPS rating of a CPU <MI/s>
	public static final int CLOUD_PM_RAM								= 32768;	// RAM physical memory <MB>			// 32 GB
	public static final long CLOUD_PM_BANDWIDTH							= 80;		// Bandwidth <MB/s>
	public static final long CLOUD_PM_STORAGE							= 100000;	// Storage <MB>

	/* Raspberry Pi edge device parameters */
	public static final int RASPBERRY_PI_PM_PES_NUMBER					= 1;		// Number of CPUs
	public static final double RASPBERRY_PI_PM_MIPS						= 80000;	// MIPS rating of a CPU <MI/s>
	public static final int RASPBERRY_PI_PM_RAM							= 1024;		// RAM physical memory <MB>			// 1 GB
	public static final long RASPBERRY_PI_PM_BANDWIDTH					= 5;		// Bandwidth <MB/s>
	public static final long RASPBERRY_PI_PM_STORAGE					= 100000;	// Storage <MB>
	public static final int RASPBERRY_PI_PM_IOTDEVICE_CAPACITY			= 1000;
	public static final double RASPBERRY_PI_PM_BATTERY_MAX_CAPACITY		= 10800;
	public static final double RASPBERRY_PI_PM_BATTERY_DRAINAGE_RATE	= 0.6;		// Transfer rate: 3/5 (3h processing or 5h transfer) 
	public static final double RASPBERRY_PI_PM_BATTERY_CURRENT_CAPACITY	= 10800;

	/* Smartphone edge device parameters */
	public static final int SMARTPHONE_PM_PES_NUMBER					= 1;		// Number of CPUs
	public static final double SMARTPHONE_PM_MIPS						= 400000;	// MIPS rating of a CPU <MI/s>
	public static final int SMARTPHONE_PM_RAM							= 4096;		// RAM physical memory <MB>			// 4 GB
	public static final long SMARTPHONE_PM_BANDWIDTH					= 20;		// Bandwidth <MB/s>
	public static final long SMARTPHONE_PM_STORAGE						= 100000;	// Storage <MB>
	public static final int SMARTPHONE_PM_IOTDEVICE_CAPACITY			= 1000;
	public static final double SMARTPHONE_PM_BATTERY_MAX_CAPACITY		= 18000;
	public static final double SMARTPHONE_PM_BATTERY_DRAINAGE_RATE		= 0.625;	// Transfer rate: 5/8 (5h processing or 8h transfer) 
	public static final double SMARTPHONE_PM_BATTERY_CURRENT_CAPACITY	= 18000;

	/* Cloud virtual machine parameters (equal to cloud physical machine parameters) */
	public static final int CLOUD_VM_PES_NUMBER			= 1;			// Number of CPUs
	public static final double CLOUD_VM_MIPS			= 1000000;		// MIPS rating of a CPU <MI/s>
	public static final int CLOUD_VM_RAM				= 32768;		// VM memory <MB>								// 32 GB
	public static final long CLOUD_VM_BANDWIDTH			= 80;			// Bandwidth <MB/s>
	public static final long CLOUD_VM_STORAGE			= 100000;		// Storage/ image size <MB>

	/* Raspberry Pi resource parameters (equal to Raspberry Pi edge device parameters) */
	public static final int RASPBERRY_PI_VM_PES_NUMBER	= 1;			// Number of CPUs
	public static final double RASPBERRY_PI_VM_MIPS		= 80000;		// MIPS rating of a CPU <MI/s>
	public static final int RASPBERRY_PI_VM_RAM			= 1024;			// VM memory <MB>
	public static final long RASPBERRY_PI_VM_BANDWIDTH	= 5;			// Bandwidth <MB/s>
	public static final long RASPBERRY_PI_VM_STORAGE	= 100000;		// Storage/ image size <MB>

	/* Smartphone resource parameters (equal to Smartphone edge device parameters) */
	public static final int SMARTPHONE_VM_PES_NUMBER	= 1;			// Number of CPUs
	public static final double SMARTPHONE_VM_MIPS		= 400000;		// MIPS rating of a CPU <MI/s>
	public static final int SMARTPHONE_VM_RAM			= 4096;			// VM memory <MB>
	public static final long SMARTPHONE_VM_BANDWIDTH	= 20;			// Bandwidth <MB/s>
	public static final long SMARTPHONE_VM_STORAGE		= 100000;		// Storage/ image size <MB>

	/* Datacenter characteristics */
	public static final String ARCHITECTURE				= "x86";		// System architecture
	public static final String OS						= "Linux";		// Operating System
	public static final String VMM						= "Xen";		// Virtual Machine Monitor

	/* ------------------- Task parameters ------------------- */
	public static final double ARRIVAL_RATE				= 0.8;
	public static final int TASK_PES_NUMBER				= 1;			// Number of CPUs required to execute the task

	/* Read tasks */
	public static final double READ_TASK1_LENGTH		= 2000000;		// Task length <MI>
	public static final double READ_TASK1_FILESIZE		= 5000;			// File size of task before execution <MB>		// 5 GB
	public static final double READ_TASK1_OUTPUTSIZE	= 0;			// File size of task after execution <MB>

	public static final double READ_TASK2_LENGTH		= 4000000;
	public static final double READ_TASK2_FILESIZE		= 200;															// 0.2 GB
	public static final double READ_TASK2_OUTPUTSIZE	= 0;

	public static final double READ_TASK3_LENGTH		= 200000;
	public static final double READ_TASK3_FILESIZE		= 5000;															// 5 GB
	public static final double READ_TASK3_OUTPUTSIZE	= 0;

	public static final double READ_TASK4_LENGTH		= 500000;
	public static final double READ_TASK4_FILESIZE		= 500;															// 0.5 GB
	public static final double READ_TASK4_OUTPUTSIZE	= 0;

	/* Write tasks */
	public static final double WRITE_TASK1_LENGTH		= 2000000;		// Task length <MI>
	public static final double WRITE_TASK1_FILESIZE		= 0;			// File size of task before execution <MB>
	public static final double WRITE_TASK1_OUTPUTSIZE	= 2000;			// File size of task after execution <MB>		// 2 GB

	public static final double WRITE_TASK2_LENGTH		= 1000000;
	public static final double WRITE_TASK2_FILESIZE		= 0;
	public static final double WRITE_TASK2_OUTPUTSIZE	= 500;															// 0.5 GB

	public static final double WRITE_TASK3_LENGTH		= 500000;
	public static final double WRITE_TASK3_FILESIZE		= 0;
	public static final double WRITE_TASK3_OUTPUTSIZE	= 5000;															// 5 GB

	public static final double WRITE_TASK4_LENGTH		= 200000;
	public static final double WRITE_TASK4_FILESIZE		= 0;
	public static final double WRITE_TASK4_OUTPUTSIZE	= 200;															// 0.2 GB

}
