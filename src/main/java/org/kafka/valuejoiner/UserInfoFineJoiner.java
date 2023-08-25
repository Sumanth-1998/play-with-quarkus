package org.kafka.valuejoiner;

import avro.Fine;
import avro.UserFineDto;
import avro.UserInformation;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.List;

public class UserInfoFineJoiner implements ValueJoiner<UserInformation, List<Fine>, UserFineDto> {
    @Override
    public UserFineDto apply(UserInformation userInformation, List<Fine> fine) {
        UserFineDto userFineDto = new UserFineDto();
        userFineDto.setName(userInformation.getName());
        userFineDto.setEmail(userInformation.getEmail());
        userFineDto.setDob(userInformation.getDob());
        userFineDto.setSsn(userInformation.getSsn());
        userFineDto.setFines(fine);
        return userFineDto;
    }
}
