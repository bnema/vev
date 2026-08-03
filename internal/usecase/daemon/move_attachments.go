package daemon

// frozenMoveAttachmentRetirement records that the exact source attachment
// effect gate was frozen before a final move removes its session membership.
// Other registered attachments are independent and are never retired here.
type frozenMoveAttachmentRetirement struct{}
