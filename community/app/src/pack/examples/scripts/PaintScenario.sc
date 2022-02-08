import com.digitalasset.canton.examples.{Iou, Paint}
import com.digitalasset.canton.ledger.api.client.DecodeUtil
import com.digitalasset.canton.console.LocalParticipantReference

def run(
    participant1: LocalParticipantReference,
    participant2: LocalParticipantReference,
    alice: PartyId, bob: PartyId) = {

  val bank = participant2.parties.enable("Bank", waitForDomain = DomainChoice.All)

  utils.retry_until_true() {
    mydomain.parties.list("Bank").nonEmpty
  }

  val amount = Iou.Amount(100, "USD")
  val iouCreateCmd = Iou.Iou(
    payer = bank.toPrim,
    owner = alice.toPrim,
    amount = amount,
    viewers = List.empty
  ).create.command
  val iouTx = participant2.ledger_api.commands.submit_flat(Seq(bank), Seq(iouCreateCmd))
  val iou = DecodeUtil.decodeAllCreated(Iou.Iou)(iouTx).head

  val offerCreateCmd = Paint.OfferToPaintHouseByPainter(
    houseOwner = alice.toPrim,
    painter = bob.toPrim,
    bank = bank.toPrim,
    amount = amount
  ).create.command

  val offer = DecodeUtil.decodeAllCreated(Paint.OfferToPaintHouseByPainter)(
    participant2.ledger_api.commands.submit_flat(Seq(bob), Seq(offerCreateCmd))
  ).head

  val acceptanceCmd = offer.contractId.exerciseAcceptByOwner(
    actor=alice.toPrim,
    iouId=iou.contractId
  ).command
  val bobIou = DecodeUtil.decodeAllCreatedTree(Iou.Iou)(
    participant1.ledger_api.commands.submit(Seq(alice), Seq(acceptanceCmd))
  ).head

  val callCmd = bobIou.contractId.exerciseCall(bob.toPrim).command
  val called = DecodeUtil.decodeAllCreated(Iou.GetCash)(
    participant2.ledger_api.commands.submit_flat(Seq(bob), Seq(callCmd))
  ).head
}
