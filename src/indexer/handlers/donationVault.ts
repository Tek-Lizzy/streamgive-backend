import { scValToNative } from '@stellar/stellar-sdk';

import { prisma } from '../../db.js';
import { notify } from '../../notifications/service.js';
import type { ContractEvent } from '../worker.js';

async function ensureDonor(address: string) {
  return prisma.donor.upsert({
    where: { address },
    create: { address },
    update: {},
  });
}

async function ensureNgo(ownerAddress: string) {
  return prisma.ngo.upsert({
    where: { ownerAddress },
    // A stream can reference an NGO address that hasn't gone through
    // ngo-registry — donation-vault doesn't check registry membership
    // on-chain. Placeholder name until (if) a "register" event arrives;
    // `update: {}` below makes sure we never clobber a real name/verified
    // status that's already on file.
    create: { ownerAddress, name: '', verified: false },
    update: {},
  });
}

/**
 * Handles the donation-vault contract's `created` event, emitted when a
 * donor opens a new streaming donation to an NGO.
 *
 * Writes: upserts the `donor` and `ngo` rows (creating them if this is the
 * first time either address has been seen), then upserts a `stream` row
 * with status `ACTIVE`, the initial deposit as `balance`, and `withdrawn`
 * set to `0`. Persists a `StreamEvent` row in the same transaction.
 * Emits a `stream_created` notification.
 */
async function handleStreamCreated(event: ContractEvent): Promise<void> {
  const [, streamIdVal] = event.topic;
  const onChainId = scValToNative(streamIdVal) as bigint;

  const [donorVal, ngoVal, tokenVal, depositVal, rateVal] = scValToNative(event.value) as [
    { toString(): string },
    { toString(): string },
    { toString(): string },
    bigint,
    bigint,
  ];

  const [donor, ngo] = await Promise.all([
    ensureDonor(donorVal.toString()),
    ensureNgo(ngoVal.toString()),
  ]);

  await prisma.$transaction(async (tx) => {
    await tx.stream.upsert({
      where: { onChainId },
      create: {
        onChainId,
        donorId: donor.id,
        ngoId: ngo.id,
        tokenAddress: tokenVal.toString(),
        rate: rateVal.toString(),
        balance: depositVal.toString(),
        withdrawn: '0',
        status: 'ACTIVE',
        createdAt: new Date(event.ledgerClosedAt),
      },
      update: {},
    });

    await tx.streamEvent.create({
      data: {
        type: 'created',
        streamId: onChainId,
        ledger: event.ledger,
        txHash: event.txHash,
        payload: {
          donor: donorVal.toString(),
          ngo: ngoVal.toString(),
          token: tokenVal.toString(),
          deposit: depositVal.toString(),
          rate: rateVal.toString(),
        },
      },
    });
  });

  await notify({
    type: 'stream_created',
    eventId: event.id,
    streamId: onChainId.toString(),
    donorAddress: donorVal.toString(),
    ngoId: ngo.id,
  });
}

/**
 * Handles the donation-vault contract's `withdraw` event, emitted when
 * accrued funds are withdrawn to the NGO from an active stream.
 *
 * Writes: updates the matching `stream` row, subtracting the accrued
 * amount from `balance` and adding it to `withdrawn`. Persists a
 * `StreamEvent` row in the same transaction. No-ops if the stream isn't
 * known yet. Emits a `stream_withdrawn` notification.
 *
 * Withdraw's event payload is just the accrued amount, so the new balance
 * and withdrawn total are fully determined by it — no ambiguity.
 */
async function handleWithdraw(event: ContractEvent): Promise<void> {
  const [, streamIdVal] = event.topic;
  const onChainId = scValToNative(streamIdVal) as bigint;
  const accrued = scValToNative(event.value) as bigint;

  const stream = await prisma.stream.findUnique({ where: { onChainId } });
  if (!stream) {
    console.warn(`[indexer] Warning: withdraw event for unknown stream ${onChainId}`);
    return;
  }

  await prisma.$transaction(async (tx) => {
    await tx.stream.update({
      where: { onChainId },
      data: {
        balance: (BigInt(stream.balance) - accrued).toString(),
        withdrawn: (BigInt(stream.withdrawn) + accrued).toString(),
        updatedAt: new Date(event.ledgerClosedAt),
      },
    });

    await tx.streamEvent.create({
      data: {
        type: 'withdraw',
        streamId: onChainId,
        ledger: event.ledger,
        txHash: event.txHash,
        payload: { accrued: accrued.toString() },
      },
    });
  });

  await notify({
    type: 'stream_withdrawn',
    eventId: event.id,
    streamId: onChainId.toString(),
    amount: accrued.toString(),
  });
}

/**
 * Handles the donation-vault contract's `cancel` event, emitted when a
 * stream is cancelled, settling accrued funds to the NGO and refunding the
 * remaining balance to the donor.
 *
 * Writes: updates the matching `stream` row — adds the settled amount to
 * `withdrawn`, zeroes `balance` and `rate`, and sets `status` to
 * `CANCELLED`. Persists a `StreamEvent` row in the same transaction.
 * No-ops if the stream isn't known yet. Emits a `stream_cancelled`
 * notification.
 *
 * Cancel's payload carries both the settled amount and the refund, so —
 * like withdraw — the resulting state is fully determined by the event.
 */
async function handleCancel(event: ContractEvent): Promise<void> {
  const [, streamIdVal] = event.topic;
  const onChainId = scValToNative(streamIdVal) as bigint;
  const [accrued, refund] = scValToNative(event.value) as [bigint, bigint];

  const stream = await prisma.stream.findUnique({ where: { onChainId } });
  if (!stream) {
    console.warn(`[indexer] Warning: cancel event for unknown stream ${onChainId}`);
    return;
  }

  await prisma.$transaction(async (tx) => {
    await tx.stream.update({
      where: { onChainId },
      data: {
        withdrawn: (BigInt(stream.withdrawn) + accrued).toString(),
        balance: '0',
        lastRate: stream.rate,
        rate: '0',
        status: 'CANCELLED',
        updatedAt: new Date(event.ledgerClosedAt),
      },
    });

    await tx.streamEvent.create({
      data: {
        type: 'cancel',
        streamId: onChainId,
        ledger: event.ledger,
        txHash: event.txHash,
        payload: {
          accrued: accrued.toString(),
          refund: refund.toString(),
        },
      },
    });
  });

  await notify({
    type: 'stream_cancelled',
    eventId: event.id,
    streamId: onChainId.toString(),
    settledToNgo: accrued.toString(),
    refundToDonor: refund.toString(),
  });
}

/**
 * Entry point for all donation-vault contract events. Dispatches on the
 * event's topic to the handler for that on-chain event:
 * `created` → {@link handleStreamCreated}, `withdraw` → {@link handleWithdraw},
 * `cancel` → {@link handleCancel}. `topup` and `ratemod` are deliberately
 * unhandled for now (see comment below); any other topic is ignored.
 */
export async function handleDonationVaultEvent(event: ContractEvent): Promise<void> {
  const [topicSymbol] = event.topic;
  const topic = scValToNative(topicSymbol) as string;

  switch (topic) {
    case 'created':
      await handleStreamCreated(event);
      break;
    case 'withdraw':
      await handleWithdraw(event);
      break;
    case 'cancel':
      await handleCancel(event);
      break;
    case 'topup':
    case 'ratemod':
      // Deliberately unhandled for now: both events only publish the new
      // amount/rate, not how much accrued and settled to the NGO during
      // the same call, so the new balance can't be reconstructed from the
      // event payload alone without either a verified read-only contract
      // call (get_stream) or duplicating the contract's accrual math here
      // — both real work, tracked as follow-up rather than guessed at.
      break;
    default:
      break;
  }
}
