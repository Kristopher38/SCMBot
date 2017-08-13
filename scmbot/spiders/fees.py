import math

def CalculateAmountToSendForDesiredReceivedAmount(receivedAmount, publisherFee):
	nSteamFee = int(math.floor(max([receivedAmount *  0.05, 1])))
	nPublisherFee = int(math.floor(max([receivedAmount * publisherFee, 1]) if publisherFee > 0 else 0))
	nAmountToSend = receivedAmount + nSteamFee + nPublisherFee
	return {
		'steam_fee': nSteamFee,
		'publisher_fee': nPublisherFee,
		'fees': nSteamFee + nPublisherFee,
		'amount': int(nAmountToSend)
	}
		
def calculate_fee_amount(amount, publisherFee):
	iterations = 0
	nEstimatedAmountOfWalletFundsReceivedByOtherParty = int(( amount ) / ( 0.05 + publisherFee + 1 ))
	bEverUndershot = False
	fees = CalculateAmountToSendForDesiredReceivedAmount( nEstimatedAmountOfWalletFundsReceivedByOtherParty, publisherFee )
	
	while fees['amount'] != amount and iterations < 10:
		if fees['amount'] > amount:
			if bEverUndershot == True:
				fees = CalculateAmountToSendForDesiredReceivedAmount( nEstimatedAmountOfWalletFundsReceivedByOtherParty - 1, publisherFee )
				fees['steam_fee'] += amount - fees['amount']
				fees['fees'] += amount - fees['amount']
				fees['amount'] = amount
				break
			else:
				nEstimatedAmountOfWalletFundsReceivedByOtherParty -= 1
		else:
			bEverUndershot = True
			nEstimatedAmountOfWalletFundsReceivedByOtherParty += 1
		fees = CalculateAmountToSendForDesiredReceivedAmount( nEstimatedAmountOfWalletFundsReceivedByOtherParty, publisherFee )
		iterations += 1
		
	return fees