const valid_events = [
  {
    event: 'init',
    sessionId: '', // if not provided the server MUST generate it
    heartbeatInterval: 30, // if not provided the server MUST generate it
    timestamp: -1,
    playhead: -1, // if the player has an expected startTime, eg. if user continues watches a movie, use that value here.
    duration: -1,
    payload: {
      live: false,
      contentId: '',
      contentUrl: '',
      drmType: '',
      userId: '',
      deviceId: '',
      deviceModel: '',
      deviceType: '',
    },
  },
  {
    event: 'heartbeat',
    sessionId: '123-1231233-123',
    timestamp: 0,
    playhead: 0,
    duration: 0,
    payload: {
      events: [
        {
          event: 'loading',
          timestamp: 0,
          playhead: 0,
          duration: 0,
        },
        {
          event: 'loaded',
          timestamp: 0,
          playhead: 0,
          duration: 0,
        },
      ],
    },
  },
  {
    event: 'loading',
    timestamp: 0,
    playhead: 0,
    duration: 0,
  },
  {
    event: 'loaded',
    timestamp: 0,
    playhead: 0,
    duration: 0,
  },
  {
    event: 'play',
    timestamp: 0,
    playhead: 0,
    duration: 0,
  },
  {
    event: 'pause',
    timestamp: 0,
    playhead: 0,
    duration: 0,
  },
  {
    event: 'resume',
    timestamp: 0,
    playhead: 0,
    duration: 0,
  },
  {
    event: 'buffering',
    timestamp: 0,
    playhead: 0,
    duration: 0,
  },
  {
    event: 'buffered',
    timestamp: 0,
    playhead: 0,
    duration: 0,
  },
  {
    event: 'seeking',
    timestamp: 0,
    playhead: 0,
    duration: 0,
  },
  {
    event: 'seeked',
    timestamp: 0,
    playhead: 0,
    duration: 0,
  },
  {
    event: 'bitrate_changed',
    timestamp: 0,
    playhead: 0,
    duration: 0,
    payload: {
      bitrate: 300, // bitrate in Kbps
      width: 1920, // video width in pixels
      height: 1080, // video height in pixels
      videoBitrate: 300, // if available provide the bitrate for the video track
      audioBitrate: 300, // if available provide the bitrate for the audio track
    },
  },
  {
    event: 'stopped',
    sessionId: '123-214-234',
    timestamp: 0,
    playhead: 0,
    duration: 0,
    payload: {
      reason: 'ended', // eg. "ended", "aborted", "error"
      events: [
        {
          event: 'seeked',
          timestamp: 0,
          playhead: 0,
          duration: 0,
        },
        {
          event: 'seeking',
          timestamp: 0,
          playhead: 0,
          duration: 0,
        },
      ],
    },
  },
  {
    event: 'error',
    timestamp: 0,
    playhead: 0,
    duration: 0,
    payload: {
      category: 'NETWORK', // eg. NETWORK, DECODER, etc.
      code: '',
      message: 'Network Error',
      data: {},
    },
  },
  {
    event: 'warning',
    timestamp: 0,
    playhead: 0,
    duration: 0,
    payload: {
      category: 'NETWORK', // eg. NETWORK, DECODER, osv.
      code: '404',
      message: 'Network Error',
      data: {},
    },
  },
];

const invalid_events = [
  {
    event: 'init',
    payload: {
      live: false,
      contentId: '',
      contentUrl: '',
      drmType: '',
      userId: '',
      deviceId: '',
      deviceModel: '',
      deviceType: '',
    },
  },
  {
    event: 'heartbeat',
    timestamp: 0,
    playhead: 0,
    duration: 0,
    payload: {
      events: [
        {
          event: 'loading',
          timestamp: 0,
          playhead: 0,
          duration: 0,
        },
        {
          event: 'loaded',
          timestamp: 0,
          playhead: 0,
          duration: 0,
        },
      ],
    },
  },
  {
    event: 'loading',
    timestamp: 0,
    duration: 0,
  },
  {
    event: 'loaded',
    playhead: 0,
    duration: 0,
  },
  {
    event: 'play',
    timestamp: 0,
  },
  {
    event: 'pause',
    playhead: 0,
    duration: 0,
  },
  {
    event: 'resume',
    timestamp: 0,
    duration: 0,
  },
  {
    event: 'buffering',
    playhead: 0,
    duration: 0,
  },
  {
    event: 'buffered',
    timestamp: 0,
    playhead: 0,
  },
  {
    event: 'seeking',
    playhead: 0,
    duration: 0,
  },
  {
    event: 'seeked',
  },
  {
    event: 'bitrate_changed',
    timestamp: 0,
    duration: 0,
    payload: {},
  },
  {
    event: 'stopped',
    sessionId: '123-214-234',
    duration: 0,
    playhead: 0,
    payload: {},
  },
  {
    event: 'error',
  },
  {
    event: 'warning',
    timestamp: '',
    playhead: 0,
    duration: 0,
    payload: {
      category: 'NETWORK', // eg. NETWORK, DECODER, osv.
      code: '404',
      message: 'Network Error',
      data: {},
    },
  },
];

export { valid_events, invalid_events };
