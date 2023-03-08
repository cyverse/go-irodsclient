package common

import "fmt"

// LinuxErrorCode is an error code type
type LinuxErrorCode int

var (
	linuxErrorCodeDescriptionTable = map[LinuxErrorCode]string{}
)

// error codes
const (
	EPERM           LinuxErrorCode = 1   /* Operation not permitted */
	ENOENT          LinuxErrorCode = 2   /* No such file or directory */
	ESRCH           LinuxErrorCode = 3   /* No such process */
	EINTR           LinuxErrorCode = 4   /* Interrupted system call */
	EIO             LinuxErrorCode = 5   /* I/O error */
	ENXIO           LinuxErrorCode = 6   /* No such device or address */
	E2BIG           LinuxErrorCode = 7   /* Arg list too long */
	ENOEXEC         LinuxErrorCode = 8   /* Exec format error */
	EBADF           LinuxErrorCode = 9   /* Bad file number */
	ECHILD          LinuxErrorCode = 10  /* No child processes */
	EAGAIN          LinuxErrorCode = 11  /* Try again */
	ENOMEM          LinuxErrorCode = 12  /* Out of memory */
	EACCES          LinuxErrorCode = 13  /* Permission denied */
	EFAULT          LinuxErrorCode = 14  /* Bad address */
	ENOTBLK         LinuxErrorCode = 15  /* Block device required */
	EBUSY           LinuxErrorCode = 16  /* Device or resource busy */
	EEXIST          LinuxErrorCode = 17  /* File exists */
	EXDEV           LinuxErrorCode = 18  /* Cross-device link */
	ENODEV          LinuxErrorCode = 19  /* No such device */
	ENOTDIR         LinuxErrorCode = 20  /* Not a directory */
	EISDIR          LinuxErrorCode = 21  /* Is a directory */
	EINVAL          LinuxErrorCode = 22  /* Invalid argument */
	ENFILE          LinuxErrorCode = 23  /* File table overflow */
	EMFILE          LinuxErrorCode = 24  /* Too many open files */
	ENOTTY          LinuxErrorCode = 25  /* Not a typewriter */
	ETXTBSY         LinuxErrorCode = 26  /* Text file busy */
	EFBIG           LinuxErrorCode = 27  /* File too large */
	ENOSPC          LinuxErrorCode = 28  /* No space left on device */
	ESPIPE          LinuxErrorCode = 29  /* Illegal seek */
	EROFS           LinuxErrorCode = 30  /* Read-only file system */
	EMLINK          LinuxErrorCode = 31  /* Too many links */
	EPIPE           LinuxErrorCode = 32  /* Broken pipe */
	EDOM            LinuxErrorCode = 33  /* Math argument out of domain of func */
	ERANGE          LinuxErrorCode = 34  /* Math result not representable */
	EDEADLK         LinuxErrorCode = 35  /* Resource deadlock would occur */
	ENAMETOOLONG    LinuxErrorCode = 36  /* File name too long */
	ENOLCK          LinuxErrorCode = 37  /* No record locks available */
	ENOSYS          LinuxErrorCode = 38  /* Function not implemented */
	ENOTEMPTY       LinuxErrorCode = 39  /* Directory not empty */
	ELOOP           LinuxErrorCode = 40  /* Too many symbolic links encountered */
	ENOMSG          LinuxErrorCode = 42  /* No message of desired type */
	EIDRM           LinuxErrorCode = 43  /* Identifier removed */
	ECHRNG          LinuxErrorCode = 44  /* Channel number out of range */
	EL2NSYNC        LinuxErrorCode = 45  /* Level LinuxErrorCode = 2 not synchronized */
	EL3HLT          LinuxErrorCode = 46  /* Level LinuxErrorCode = 3 halted */
	EL3RST          LinuxErrorCode = 47  /* Level LinuxErrorCode = 3 reset */
	ELNRNG          LinuxErrorCode = 48  /* Link number out of range */
	EUNATCH         LinuxErrorCode = 49  /* Protocol driver not attached */
	ENOCSI          LinuxErrorCode = 50  /* No CSI structure available */
	EL2HLT          LinuxErrorCode = 51  /* Level LinuxErrorCode = 2 halted */
	EBADE           LinuxErrorCode = 52  /* Invalid exchange */
	EBADR           LinuxErrorCode = 53  /* Invalid request descriptor */
	EXFULL          LinuxErrorCode = 54  /* Exchange full */
	ENOANO          LinuxErrorCode = 55  /* No anode */
	EBADRQC         LinuxErrorCode = 56  /* Invalid request code */
	EBADSLT         LinuxErrorCode = 57  /* Invalid slot */
	EBFONT          LinuxErrorCode = 59  /* Bad font file format */
	ENOSTR          LinuxErrorCode = 60  /* Device not a stream */
	ENODATA         LinuxErrorCode = 61  /* No data available */
	ETIME           LinuxErrorCode = 62  /* Timer expired */
	ENOSR           LinuxErrorCode = 63  /* Out of streams resources */
	ENONET          LinuxErrorCode = 64  /* Machine is not on the network */
	ENOPKG          LinuxErrorCode = 65  /* Package not installed */
	EREMOTE         LinuxErrorCode = 66  /* Object is remote */
	ENOLINK         LinuxErrorCode = 67  /* Link has been severed */
	EADV            LinuxErrorCode = 68  /* Advertise error */
	ESRMNT          LinuxErrorCode = 69  /* Srmount error */
	ECOMM           LinuxErrorCode = 70  /* Communication error on send */
	EPROTO          LinuxErrorCode = 71  /* Protocol error */
	EMULTIHOP       LinuxErrorCode = 72  /* Multihop attempted */
	EDOTDOT         LinuxErrorCode = 73  /* RFS specific error */
	EBADMSG         LinuxErrorCode = 74  /* Not a data message */
	EOVERFLOW       LinuxErrorCode = 75  /* Value too large for defined data type */
	ENOTUNIQ        LinuxErrorCode = 76  /* Name not unique on network */
	EBADFD          LinuxErrorCode = 77  /* File descriptor in bad state */
	EREMCHG         LinuxErrorCode = 78  /* Remote address changed */
	ELIBACC         LinuxErrorCode = 79  /* Can not access a needed shared library */
	ELIBBAD         LinuxErrorCode = 80  /* Accessing a corrupted shared library */
	ELIBSCN         LinuxErrorCode = 81  /* .lib section in a.out corrupted */
	ELIBMAX         LinuxErrorCode = 82  /* Attempting to link in too many shared libraries */
	ELIBEXEC        LinuxErrorCode = 83  /* Cannot exec a shared library directly */
	EILSEQ          LinuxErrorCode = 84  /* Illegal byte sequence */
	ERESTART        LinuxErrorCode = 85  /* Interrupted system call should be restarted */
	ESTRPIPE        LinuxErrorCode = 86  /* Streams pipe error */
	EUSERS          LinuxErrorCode = 87  /* Too many users */
	ENOTSOCK        LinuxErrorCode = 88  /* Socket operation on non-socket */
	EDESTADDRREQ    LinuxErrorCode = 89  /* Destination address required */
	EMSGSIZE        LinuxErrorCode = 90  /* Message too long */
	EPROTOTYPE      LinuxErrorCode = 91  /* Protocol wrong type for socket */
	ENOPROTOOPT     LinuxErrorCode = 92  /* Protocol not available */
	EPROTONOSUPPORT LinuxErrorCode = 93  /* Protocol not supported */
	ESOCKTNOSUPPORT LinuxErrorCode = 94  /* Socket type not supported */
	EOPNOTSUPP      LinuxErrorCode = 95  /* Operation not supported on transport endpoint */
	EPFNOSUPPORT    LinuxErrorCode = 96  /* Protocol family not supported */
	EAFNOSUPPORT    LinuxErrorCode = 97  /* Address family not supported by protocol */
	EADDRINUSE      LinuxErrorCode = 98  /* Address already in use */
	EADDRNOTAVAIL   LinuxErrorCode = 99  /* Cannot assign requested address */
	ENETDOWN        LinuxErrorCode = 100 /* Network is down */
	ENETUNREACH     LinuxErrorCode = 101 /* Network is unreachable */
	ENETRESET       LinuxErrorCode = 102 /* Network dropped connection because of reset */
	ECONNABORTED    LinuxErrorCode = 103 /* Software caused connection abort */
	ECONNRESET      LinuxErrorCode = 104 /* Connection reset by peer */
	ENOBUFS         LinuxErrorCode = 105 /* No buffer space available */
	EISCONN         LinuxErrorCode = 106 /* Transport endpoint is already connected */
	ENOTCONN        LinuxErrorCode = 107 /* Transport endpoint is not connected */
	ESHUTDOWN       LinuxErrorCode = 108 /* Cannot send after transport endpoint shutdown */
	ETOOMANYREFS    LinuxErrorCode = 109 /* Too many references: cannot splice */
	ETIMEDOUT       LinuxErrorCode = 110 /* Connection timed out */
	ECONNREFUSED    LinuxErrorCode = 111 /* Connection refused */
	EHOSTDOWN       LinuxErrorCode = 112 /* Host is down */
	EHOSTUNREACH    LinuxErrorCode = 113 /* No route to host */
	EALREADY        LinuxErrorCode = 114 /* Operation already in progress */
	EINPROGRESS     LinuxErrorCode = 115 /* Operation now in progress */
	ESTALE          LinuxErrorCode = 116 /* Stale NFS file handle */
	EUCLEAN         LinuxErrorCode = 117 /* Structure needs cleaning */
	ENOTNAM         LinuxErrorCode = 118 /* Not a XENIX named type file */
	ENAVAIL         LinuxErrorCode = 119 /* No XENIX semaphores available */
	EISNAM          LinuxErrorCode = 120 /* Is a named type file */
	EREMOTEIO       LinuxErrorCode = 121 /* Remote I/O error */
	EDQUOT          LinuxErrorCode = 122 /* Quota exceeded */
	ENOMEDIUM       LinuxErrorCode = 123 /* No medium found */
	EMEDIUMTYPE     LinuxErrorCode = 124 /* Wrong medium type */
	ECANCELED       LinuxErrorCode = 125 /* Operation Cancelled */
	ENOKEY          LinuxErrorCode = 126 /* Required key not available */
	EKEYEXPIRED     LinuxErrorCode = 127 /* Key has expired */
	EKEYREVOKED     LinuxErrorCode = 128 /* Key has been revoked */
	EKEYREJECTED    LinuxErrorCode = 129 /* Key was rejected by service */
)

func init() {
	linuxErrorCodeDescriptionTable[EPERM] = "Operation not permitted"
	linuxErrorCodeDescriptionTable[ENOENT] = "No such file or directory"
	linuxErrorCodeDescriptionTable[ESRCH] = "No such process"
	linuxErrorCodeDescriptionTable[EINTR] = "Interrupted system call"
	linuxErrorCodeDescriptionTable[EIO] = "I/O error"
	linuxErrorCodeDescriptionTable[ENXIO] = "No such device or address"
	linuxErrorCodeDescriptionTable[E2BIG] = "Arg list too long"
	linuxErrorCodeDescriptionTable[ENOEXEC] = "Exec format error"
	linuxErrorCodeDescriptionTable[EBADF] = "Bad file number"
	linuxErrorCodeDescriptionTable[ECHILD] = "No child processes"
	linuxErrorCodeDescriptionTable[EAGAIN] = "Try again"
	linuxErrorCodeDescriptionTable[ENOMEM] = "Out of memory"
	linuxErrorCodeDescriptionTable[EACCES] = "Permission denied"
	linuxErrorCodeDescriptionTable[EFAULT] = "Bad address"
	linuxErrorCodeDescriptionTable[ENOTBLK] = "Block device required"
	linuxErrorCodeDescriptionTable[EBUSY] = "Device or resource busy"
	linuxErrorCodeDescriptionTable[EEXIST] = "File exists"
	linuxErrorCodeDescriptionTable[EXDEV] = "Cross-device link"
	linuxErrorCodeDescriptionTable[ENODEV] = "No such device"
	linuxErrorCodeDescriptionTable[ENOTDIR] = "Not a directory"
	linuxErrorCodeDescriptionTable[EISDIR] = "Is a directory"
	linuxErrorCodeDescriptionTable[EINVAL] = "Invalid argument"
	linuxErrorCodeDescriptionTable[ENFILE] = "File table overflow"
	linuxErrorCodeDescriptionTable[EMFILE] = "Too many open files"
	linuxErrorCodeDescriptionTable[ENOTTY] = "Not a typewriter"
	linuxErrorCodeDescriptionTable[ETXTBSY] = "Text file busy"
	linuxErrorCodeDescriptionTable[EFBIG] = "File too large"
	linuxErrorCodeDescriptionTable[ENOSPC] = "No space left on device"
	linuxErrorCodeDescriptionTable[ESPIPE] = "Illegal seek"
	linuxErrorCodeDescriptionTable[EROFS] = "Read-only file system"
	linuxErrorCodeDescriptionTable[EMLINK] = "Too many links"
	linuxErrorCodeDescriptionTable[EPIPE] = "Broken pipe"
	linuxErrorCodeDescriptionTable[EDOM] = "Math argument out of domain of func"
	linuxErrorCodeDescriptionTable[ERANGE] = "Math result not representable"
	linuxErrorCodeDescriptionTable[EDEADLK] = "Resource deadlock would occur"
	linuxErrorCodeDescriptionTable[ENAMETOOLONG] = "File name too long"
	linuxErrorCodeDescriptionTable[ENOLCK] = "No record locks available"
	linuxErrorCodeDescriptionTable[ENOSYS] = "Function not implemented"
	linuxErrorCodeDescriptionTable[ENOTEMPTY] = "Directory not empty"
	linuxErrorCodeDescriptionTable[ELOOP] = "Too many symbolic links encountered"
	linuxErrorCodeDescriptionTable[ENOMSG] = "No message of desired type"
	linuxErrorCodeDescriptionTable[EIDRM] = "Identifier removed"
	linuxErrorCodeDescriptionTable[ECHRNG] = "Channel number out of range"
	linuxErrorCodeDescriptionTable[EL2NSYNC] = "Level LinuxErrorCode = 2 not synchronized"
	linuxErrorCodeDescriptionTable[EL3HLT] = "Level LinuxErrorCode = 3 halted"
	linuxErrorCodeDescriptionTable[EL3RST] = "Level LinuxErrorCode = 3 reset"
	linuxErrorCodeDescriptionTable[ELNRNG] = "Link number out of range"
	linuxErrorCodeDescriptionTable[EUNATCH] = "Protocol driver not attached"
	linuxErrorCodeDescriptionTable[ENOCSI] = "No CSI structure available"
	linuxErrorCodeDescriptionTable[EL2HLT] = "Level LinuxErrorCode = 2 halted"
	linuxErrorCodeDescriptionTable[EBADE] = "Invalid exchange"
	linuxErrorCodeDescriptionTable[EBADR] = "Invalid request descriptor"
	linuxErrorCodeDescriptionTable[EXFULL] = "Exchange full"
	linuxErrorCodeDescriptionTable[ENOANO] = "No anode"
	linuxErrorCodeDescriptionTable[EBADRQC] = "Invalid request code"
	linuxErrorCodeDescriptionTable[EBADSLT] = "Invalid slot"
	linuxErrorCodeDescriptionTable[EBFONT] = "Bad font file format"
	linuxErrorCodeDescriptionTable[ENOSTR] = "Device not a stream"
	linuxErrorCodeDescriptionTable[ENODATA] = "No data available"
	linuxErrorCodeDescriptionTable[ETIME] = "Timer expired"
	linuxErrorCodeDescriptionTable[ENOSR] = "Out of streams resources"
	linuxErrorCodeDescriptionTable[ENONET] = "Machine is not on the network"
	linuxErrorCodeDescriptionTable[ENOPKG] = "Package not installed"
	linuxErrorCodeDescriptionTable[EREMOTE] = "Object is remote"
	linuxErrorCodeDescriptionTable[ENOLINK] = "Link has been severed"
	linuxErrorCodeDescriptionTable[EADV] = "Advertise error"
	linuxErrorCodeDescriptionTable[ESRMNT] = "Srmount error"
	linuxErrorCodeDescriptionTable[ECOMM] = "Communication error on send"
	linuxErrorCodeDescriptionTable[EPROTO] = "Protocol error"
	linuxErrorCodeDescriptionTable[EMULTIHOP] = "Multihop attempted"
	linuxErrorCodeDescriptionTable[EDOTDOT] = "RFS specific error"
	linuxErrorCodeDescriptionTable[EBADMSG] = "Not a data message"
	linuxErrorCodeDescriptionTable[EOVERFLOW] = "Value too large for defined data type"
	linuxErrorCodeDescriptionTable[ENOTUNIQ] = "Name not unique on network"
	linuxErrorCodeDescriptionTable[EBADFD] = "File descriptor in bad state"
	linuxErrorCodeDescriptionTable[EREMCHG] = "Remote address changed"
	linuxErrorCodeDescriptionTable[ELIBACC] = "Can not access a needed shared library"
	linuxErrorCodeDescriptionTable[ELIBBAD] = "Accessing a corrupted shared library"
	linuxErrorCodeDescriptionTable[ELIBSCN] = ".lib section in a.out corrupted"
	linuxErrorCodeDescriptionTable[ELIBMAX] = "Attempting to link in too many shared libraries"
	linuxErrorCodeDescriptionTable[ELIBEXEC] = "Cannot exec a shared library directly"
	linuxErrorCodeDescriptionTable[EILSEQ] = "Illegal byte sequence"
	linuxErrorCodeDescriptionTable[ERESTART] = "Interrupted system call should be restarted"
	linuxErrorCodeDescriptionTable[ESTRPIPE] = "Streams pipe error"
	linuxErrorCodeDescriptionTable[EUSERS] = "Too many users"
	linuxErrorCodeDescriptionTable[ENOTSOCK] = "Socket operation on non-socket"
	linuxErrorCodeDescriptionTable[EDESTADDRREQ] = "Destination address required"
	linuxErrorCodeDescriptionTable[EMSGSIZE] = "Message too long"
	linuxErrorCodeDescriptionTable[EPROTOTYPE] = "Protocol wrong type for socket"
	linuxErrorCodeDescriptionTable[ENOPROTOOPT] = "Protocol not available"
	linuxErrorCodeDescriptionTable[EPROTONOSUPPORT] = "Protocol not supported"
	linuxErrorCodeDescriptionTable[ESOCKTNOSUPPORT] = "Socket type not supported"
	linuxErrorCodeDescriptionTable[EOPNOTSUPP] = "Operation not supported on transport endpoint"
	linuxErrorCodeDescriptionTable[EPFNOSUPPORT] = "Protocol family not supported"
	linuxErrorCodeDescriptionTable[EAFNOSUPPORT] = "Address family not supported by protocol"
	linuxErrorCodeDescriptionTable[EADDRINUSE] = "Address already in use"
	linuxErrorCodeDescriptionTable[EADDRNOTAVAIL] = "Cannot assign requested address"
	linuxErrorCodeDescriptionTable[ENETDOWN] = "Network is down"
	linuxErrorCodeDescriptionTable[ENETUNREACH] = "Network is unreachable"
	linuxErrorCodeDescriptionTable[ENETRESET] = "Network dropped connection because of reset"
	linuxErrorCodeDescriptionTable[ECONNABORTED] = "Software caused connection abort"
	linuxErrorCodeDescriptionTable[ECONNRESET] = "Connection reset by peer"
	linuxErrorCodeDescriptionTable[ENOBUFS] = "No buffer space available"
	linuxErrorCodeDescriptionTable[EISCONN] = "Transport endpoint is already connected"
	linuxErrorCodeDescriptionTable[ENOTCONN] = "Transport endpoint is not connected"
	linuxErrorCodeDescriptionTable[ESHUTDOWN] = "Cannot send after transport endpoint shutdown"
	linuxErrorCodeDescriptionTable[ETOOMANYREFS] = "Too many references: cannot splice"
	linuxErrorCodeDescriptionTable[ETIMEDOUT] = "Connection timed out"
	linuxErrorCodeDescriptionTable[ECONNREFUSED] = "Connection refused"
	linuxErrorCodeDescriptionTable[EHOSTDOWN] = "Host is down"
	linuxErrorCodeDescriptionTable[EHOSTUNREACH] = "No route to host"
	linuxErrorCodeDescriptionTable[EALREADY] = "Operation already in progress"
	linuxErrorCodeDescriptionTable[EINPROGRESS] = "Operation now in progress"
	linuxErrorCodeDescriptionTable[ESTALE] = "Stale NFS file handle"
	linuxErrorCodeDescriptionTable[EUCLEAN] = "Structure needs cleaning"
	linuxErrorCodeDescriptionTable[ENOTNAM] = "Not a XENIX named type file"
	linuxErrorCodeDescriptionTable[ENAVAIL] = "No XENIX semaphores available"
	linuxErrorCodeDescriptionTable[EISNAM] = "Is a named type file"
	linuxErrorCodeDescriptionTable[EREMOTEIO] = "Remote I/O error"
	linuxErrorCodeDescriptionTable[EDQUOT] = "Quota exceeded"
	linuxErrorCodeDescriptionTable[ENOMEDIUM] = "No medium found"
	linuxErrorCodeDescriptionTable[EMEDIUMTYPE] = "Wrong medium type"
	linuxErrorCodeDescriptionTable[ECANCELED] = "Operation Cancelled"
	linuxErrorCodeDescriptionTable[ENOKEY] = "Required key not available"
	linuxErrorCodeDescriptionTable[EKEYEXPIRED] = "Key has expired"
	linuxErrorCodeDescriptionTable[EKEYREVOKED] = "Key has been revoked"
	linuxErrorCodeDescriptionTable[EKEYREJECTED] = "Key was rejected by service"
}

// GetLinuxErrorString returns string representation of error code
func GetLinuxErrorString(code LinuxErrorCode) string {
	if code == 0 {
		return ""
	}

	if code < 0 {
		code = -1 * code
	}

	errString, ok := linuxErrorCodeDescriptionTable[code]
	if ok {
		return errString
	}
	return fmt.Sprintf("Unknown LinuxErrorCode: %d", int(code))
}
